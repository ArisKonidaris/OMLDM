package omldm

import java.util.Properties

import BipartiteTopologyAPI.operations.CallType
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.{DataInstance, Prediction, QueryResponse, Request}
import mlAPI.math.Point
import mlAPI.parameters.ParameterDescriptor
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.{FlinkHub, FlinkPredictor, FlinkSpoke}
import omldm.utils.{Checkpointing, DefaultJobParameters}
import omldm.utils.generators.MLNodeGenerator
import omldm.utils.parsers.{DataInstanceParser, RequestParser}
import omldm.utils.parsers.dataStream.DataPointParser
import omldm.utils.parsers.requestStream.PipelineMap
import omldm.utils.partitioners.random_partitioner
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

object OMLDM_Job {

  val trainingTime: OutputTag[Any] = OutputTag[Any]("trainingTime")
  val queryResponse: OutputTag[QueryResponse] = OutputTag[QueryResponse]("QueryResponse")
  val predictions: OutputTag[Prediction] = OutputTag[Prediction]("predictionStream")

  def createProperties(brokerList: String, group_id: String)(implicit params: ParameterTool): Properties = {
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", params.get(brokerList, "localhost:9092"))
    properties.setProperty("group.flink_worker_id", group_id)
    properties
  }

  def main(args: Array[String]) {

    /** Set up the streaming execution environment */
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val params: ParameterTool = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(params.get("parallelism", DefaultJobParameters.defaultParallelism).toInt)
    utils.CommonUtils.registerFlinkMLTypes(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    if (params.get("checkpointing", "false").toBoolean) Checkpointing.enableCheckpointing()


    ////////////////////////////////////////////// Kafka Connectors ////////////////////////////////////////////////////


    /** The coordinator messages. */
    val psMessages: DataStream[HubMessage] = env.addSource(
      new FlinkKafkaConsumer[HubMessage](
        params.get("psMessagesTopic", "psMessages"),
        new TypeInformationSerializationSchema(createTypeInformation[HubMessage], env.getConfig),
        createProperties("psMessagesAddr", "psMessagesConsumer"))
        .setStartFromLatest())
      .name("FeedBackLoopSource")

    /** The incoming training data. */
    val trainingSource: DataStream[DataInstance] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("trainingDataTopic", "trainingData"),
        new SimpleStringSchema(),
        createProperties("trainingDataAddr", "trainingDataConsumer"))
        .setStartFromEarliest())
      .flatMap(DataInstanceParser())
      .name("TrainingSource")

    /** The incoming forecasting data. */
    val forecastingSource: DataStream[DataInstance] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("forecastingDataTopic", "forecastingData"),
        new SimpleStringSchema(),
        createProperties("forecastingDataAddr", "forecastingDataConsumer"))
        .setStartFromEarliest())
      .flatMap(DataInstanceParser())
      .name("ForecastingSource")

    /** The incoming requests. */
    val requests: DataStream[Request] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("requestsTopic", "requests"),
        new SimpleStringSchema(),
        createProperties("requestsAddr", "requestsConsumer"))
        .setStartFromEarliest())
      .flatMap(RequestParser())
      .name("RequestSource")


    /////////////////////////////////////////// Data and Request Parsing ///////////////////////////////////////////////


    /** Parsing the training data */
    val trainingData: DataStream[Point] = trainingSource
      .flatMap(new DataPointParser)
      .name("DataParsing")

    /** Check the validity of the request */
    val validRequest: DataStream[ControlMessage] = requests
      .keyBy((_: Request) => 0)
      .flatMap(new PipelineMap)
      .setParallelism(1)
      .name("RequestParsing")


    /////////////////////////////////////////////////// Training ///////////////////////////////////////////////////////


    /** The broadcast messages of the Hub. */
    val coordinatorMessages: DataStream[ControlMessage] = psMessages
      .flatMap(new RichFlatMapFunction[HubMessage, ControlMessage] {
        override def flatMap(in: HubMessage, out: Collector[ControlMessage]): Unit = {
          for ((rpc, dest) <- in.operations zip in.destinations)
            out.collect(ControlMessage(in.getNetworkId, rpc, in.getSource, dest, in.getData, in.getRequest))
        }
      })

    /** Partitioning the Hub's messages along with the request messages to the workers. */
    val controlMessages: DataStream[ControlMessage] = coordinatorMessages
      .partitionCustom(random_partitioner, (x: ControlMessage) => x.destination.getNodeId)
      .union(validRequest.partitionCustom(random_partitioner, (x: ControlMessage) => x.destination.getNodeId))

    /** Partitioning the training data along with the control messages to the workers. */
    val trainingDataBlocks: ConnectedStreams[Point, ControlMessage] = trainingData
      .connect(controlMessages)

    /** The parallel learning procedure happens here. */
    val worker: DataStream[SpokeMessage] = trainingDataBlocks
      .process(new FlinkSpoke[MLNodeGenerator])
      .name("FlinkSpoke")

    /** The coordinator operators, where the learners are merged. */
    val coordinator: DataStream[HubMessage] = worker
      .keyBy((x: SpokeMessage) => x.getNetworkId + "_" + x.getDestination.getNodeId)
      .process(new FlinkHub[MLNodeGenerator](params.get("test", DefaultJobParameters.defaultTestParameter).toBoolean))
      .name("FlinkHub")

    coordinator.getSideOutput(trainingTime)
      .keyBy(_ => 0)
      .process(new KeyedProcessFunction[Int, Any, Long] {

        private val timeout: Long = 15000 // The maximum waiting period.
        private var start: ValueState[Long] = _
        private var end: ValueState[Long] = _
        private var timestampState: ValueState[Long] = _ // The timestamp of the latest message.

        override def open(parameters: Configuration): Unit = {
          start = getRuntimeContext.getState(new ValueStateDescriptor[Long]("startTimestamp", classOf[Long]))
          end = getRuntimeContext.getState(new ValueStateDescriptor[Long]("endTimestamp", classOf[Long]))
          timestampState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timestampState", classOf[Long]))
        }

        override def processElement(messageTimestamp: Any,
                                    ctx: KeyedProcessFunction[Int, Any, Long]#Context, collector: Collector[Long])
        : Unit = {
          if (start.value() == null)
            start.update(messageTimestamp.asInstanceOf[Long])
          else
            end.update(messageTimestamp.asInstanceOf[Long])

          // Set the state's timestamp to the record's assigned timestamp.
          val tempTime = ctx.timestamp()
          timestampState.update(tempTime)

          // Schedule the next timer timeout ms from the current record time.
          ctx.timerService.registerEventTimeTimer(tempTime + timeout)

        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Int, Any, Long]#OnTimerContext, out: Collector[Long])
        : Unit = {
          val stateTime = timestampState.value
          // Check if this is an outdated timer or the latest timer.
          if (timestamp == stateTime + timeout)
            throw new Exception("End of Job.\nTest time: " + (end.value() - start.value()))

        }

      })

    /** The Kafka iteration for emulating parameter server messages */
    coordinator
      .addSink(new FlinkKafkaProducer[HubMessage](
        params.get("psMessagesTopic", "psMessages"), // target topic
        new TypeInformationSerializationSchema(createTypeInformation[HubMessage], env.getConfig), // serializationSchema
        {
          val props = createProperties("psMessagesAddr", "psMessagesConsumer")
          props.put("partitioner.class", "omldm.utils.kafkaPartitioners.HubMessagePartitioner")
          props
        }, // broker list & partitioner
      ))
      .name("FeedbackLoop")


    ////////////////////////////////////////////////// Predicting //////////////////////////////////////////////////////


    val modelUpdates: DataStream[ControlMessage] = coordinator
      .filter(
        {
          msg: HubMessage =>
            if ((msg.destinations.length == 0 && msg.destinations.head.getNodeId == 0) || msg.destinations.length > 0)
              true
            else
              false
        }
      ).flatMap(
        new RichFlatMapFunction[HubMessage, ControlMessage] {
          var counter: Int = 0
          override def flatMap(hMessage: HubMessage, collector: Collector[ControlMessage]): Unit = {
            if (counter == 5) {
              val message = ControlMessage(
                hMessage.getNetworkId,
                hMessage.operations.head,
                hMessage.getSource,
                hMessage.destinations.head,
                hMessage.getData,
                hMessage.getRequest
              )
              message.getOperation.setCallType(CallType.ONE_WAY)
              message.getData match {
                case _: ParameterDescriptor =>
                  for (i <- 0 until getRuntimeContext.getExecutionConfig.getParallelism) {
                    message.setDestination(new NodeId(NodeType.SPOKE, i))
                    collector.collect(message)
                  }
                case _ => println("Something went wrong while updating the predictors.")
              }
              counter = 0
            } else counter += 1
          }
        }
      ).name("ModelUpdates")

    /** Partitioning the prediction data along with the control messages to the predictors. */
    val predictionDataBlocks: ConnectedStreams[DataInstance, ControlMessage] = forecastingSource
      .flatMap(
        new RichFlatMapFunction[DataInstance, DataInstance] {
          private var count: Long = 0
          override def flatMap(in: DataInstance, collector: Collector[DataInstance]): Unit = {
            in.setId(count)
            collector.collect(in)
            if (count == getRuntimeContext.getExecutionConfig.getParallelism - 1) count = 0 else count += 1
          }
        }
      )
      .keyBy(x => x.getId)
      .connect(validRequest
        .filter(x => x.getRequest.getRequest != "Query")
        .keyBy(x => x.destination.getNodeId)
        .union(modelUpdates.keyBy(x => x.destination.getNodeId))
      )

    /** The parallel prediction procedure happens here. */
    val predictionStream: DataStream[SpokeMessage] = predictionDataBlocks
      .process(new FlinkPredictor[MLNodeGenerator])
      .name("MLPredictor")


    //////////////////////////////////////////////// Sinks /////////////////////////////////////////////////////////////


    /** A Kafka sink for the predictions. */
    predictionStream.getSideOutput(predictions)
      .map(x => x.toString)
      .addSink(new FlinkKafkaProducer[String](
        params.get("predictionsAddr", "localhost:9092"), // broker list
        params.get("predictionsTopic", "predictions"), // target topic
        new SimpleStringSchema()))
      .name("PredictionsSink")

    /** A Kafka Sink for the query responses. */
    worker.getSideOutput(queryResponse)
      .map(x => x.toString)
      .addSink(new FlinkKafkaProducer[String](
        params.get("responsesAddr", "localhost:9092"), // broker list
        params.get("responsesTopic", "responses"), // target topic
        new SimpleStringSchema()))
      .name("ResponsesSink")


    //////////////////////////////////////////// Execute OML Job ///////////////////////////////////////////////////////


    /** execute program */
    env.execute(params.get("jobName", DefaultJobParameters.defaultJobName))
  }
}
