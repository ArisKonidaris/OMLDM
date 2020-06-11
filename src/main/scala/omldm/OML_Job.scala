package omldm

import java.util.Properties

import ControlAPI.{DataInstance, QueryResponse, Request}
import mlAPI.math.Point
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.{FlinkHub, FlinkSpoke}
import omldm.utils.generators.MLNodeGenerator
import omldm.utils.parsers.dataStream.DataPointParser
import omldm.utils.parsers.requestStream.PipelineMap
import omldm.utils.parsers.{DataInstanceParser, RequestParser}
import omldm.utils.partitioners.random_partitioner
import omldm.utils.{Checkpointing, DefaultJobParameters}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

/**
  * Interactive Online Machine Learning Flink Streaming Job.
  */
object OML_Job {

  val queryResponse: OutputTag[QueryResponse] = OutputTag[QueryResponse]("QueryResponse")

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
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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
      .process(new FlinkHub[MLNodeGenerator])
      .name("FlinkHub")

    /** The Kafka iteration for emulating parameter server messages */
    coordinator
      .addSink(new FlinkKafkaProducer[HubMessage](
        params.get("psMessagesAddr", "localhost:9092"), // broker list
        params.get("psMessagesTopic", "psMessages"), // target topic
        new TypeInformationSerializationSchema(createTypeInformation[HubMessage], env.getConfig)))
      .name("FeedbackLoop")


    ////////////////////////////////////////////////// Predicting //////////////////////////////////////////////////////


//    val modelUpdates: DataStream[ControlMessage] =
//      psMessages.filter({
//        msg: ControlMessage =>
//          msg.getRequest match {
//            case Request => if (msg.getDestination.getNodeId == 0 && op == 1) true else false
//            case null => false
//          }
//      }).flatMap(
//        new RichFlatMapFunction[ControlMessage, ControlMessage] {
//
//          var counter: Int = 0
//
//          override def flatMap(message: ControlMessage, collector: Collector[ControlMessage]): Unit = {
//
//            if (counter == 5) {
//              message.getParameters match {
//                case _: Option[ParameterDescriptor] =>
//                  for (i <- 0 until getRuntimeContext.getExecutionConfig.getParallelism) {
//                    message.setWorkerID(i)
//                    collector.collect(message)
//                  }
//                case _ => println("Something went wrong while updating the predictors")
//              }
//              counter = 0
//            } else counter += 1
//
//          }
//        }
//      ).name("ModelUpdates")
//
//    /** Partitioning the prediction data along with the control messages to the predictors */
//    val predictionDataBlocks: ConnectedStreams[DataInstance, ControlMessage] = forecastingSource
//      .connect(validRequest
//        .filter(x => x.container.get.request != "Query")
//        .partitionCustom(random_partitioner, (x: ControlMessage) => x.workerID)
//        .union(
//          modelUpdates.partitionCustom(random_partitioner, (x: ControlMessage) => x.workerID)
//        ))
//
//    /** The parallel prediction procedure happens here. */
//    val predictionStream: DataStream[Prediction] = predictionDataBlocks
//      .process(new Predictor[MLNodeGenerator])
//      .name("Predictor")


    //////////////////////////////////////////////// Sinks /////////////////////////////////////////////////////////////


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