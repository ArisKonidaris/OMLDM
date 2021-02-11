package omldm

import java.util.Optional
import ControlAPI.{CountableSerial, DataInstance, Prediction, QueryResponse, Request, Statistics}
import mlAPI.math.UsablePoint
import omldm.job.FlinkLearning
import omldm.messages.{ControlMessage, HubMessage}
import omldm.utils.KafkaUtils.createProperties
import omldm.utils.kafkaPartitioners.FlinkHubMessagePartitioner
import omldm.utils.parsers.dataStream.DataPointParser
import omldm.utils.parsers.requestStream.PipelineMap
import omldm.utils.parsers.{DataInstanceParser, RequestParser}
import omldm.utils.{Checkpointing, DefaultJobParameters}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.concurrent.TimeUnit

/**
 * The Online Machine Learning and Data Mining component.
 */
object Job {

  val trainingStats: OutputTag[(String, Statistics)] = OutputTag[(String, Statistics)]("trainingStats")
  val terminationStats: OutputTag[HubMessage] = OutputTag[HubMessage]("terminationStats")
  val spokeSideOutput: OutputTag[CountableSerial] = OutputTag[CountableSerial]("SpokeSideOutput")
  val hubSideOutput: OutputTag[CountableSerial] = OutputTag[CountableSerial]("HubSideOutput")

  def runOMLDMJob(env: StreamExecutionEnvironment,
                  requests: DataStream[ControlMessage],
                  psMessages: DataStream[HubMessage]
                   )(implicit params: ParameterTool)
  : (StreamExecutionEnvironment, DataStream[HubMessage], DataStream[QueryResponse], DataStream[Prediction]) = {

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

    /** Parsing the training data. */
    val trainingData: DataStream[UsablePoint] = trainingSource
      .flatMap(new DataPointParser)
      .name("TrainingDataParsing")

    /** Parsing the forecasting data. */
    val forecastingData: DataStream[UsablePoint] = forecastingSource
      .flatMap(new DataPointParser)
      .name("ForecastingDataParsing")

    /** The unified data set stream for training and prediction. */
    val dataStream: DataStream[UsablePoint] = trainingData.union(forecastingData)

    /** Training & Prediction. */
    val distributedLearning = FlinkLearning(env, dataStream, requests, psMessages)
    val (coordinator, responses, predictions) = distributedLearning.getJobOutput

    /** The Kafka iteration for emulating parameter server messages. */
    coordinator
      .addSink(new FlinkKafkaProducer[HubMessage](
        params.get("psMessagesTopic", "psMessages"), // target topic
        new TypeInformationSerializationSchema(createTypeInformation[HubMessage], env.getConfig), // serializationSchema
        createProperties("psMessagesAddr", "psMessagesConsumer"),
        {
          val partitioner: Optional[FlinkKafkaPartitioner[HubMessage]] = Optional.of(new FlinkHubMessagePartitioner)
          partitioner
        }
      ))
      .name("FeedbackLoop")

    /** A Kafka Sink for the query responses. */
    responses
      .map(x => x.toString)
      .addSink(new FlinkKafkaProducer[String](
        params.get("responsesAddr", "localhost:9092"), // broker list
        params.get("responsesTopic", "responses"), // target topic
        new SimpleStringSchema()))
      .name("ResponsesSink")

    /** A Kafka Sink for the predictions. */
    predictions
      .map(x => x.toString)
      .addSink(new FlinkKafkaProducer[String](
        params.get("predictionsAddr", "localhost:9092"), // broker list
        params.get("predictionsTopic", "predictions"), // target topic
        new SimpleStringSchema()))
      .name("PredictionsSink")

    (env, coordinator, responses, predictions)
  }

  def main(args: Array[String]) {

    /** Set up the streaming execution environment */
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val params: ParameterTool = ParameterTool.fromArgs(args)

    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(params.get("parallelism", DefaultJobParameters.defaultParallelism).toInt)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//          3, // number of restart attempts
//          Time.of(10, TimeUnit.SECONDS) // delay
//        ))
    utils.CommonUtils.registerFlinkMLTypes(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    if (params.get("checkpointing", "false").toBoolean) Checkpointing.enableCheckpointing()


    ////////////////////////////////////////////// Kafka Sources ///////////////////////////////////////////////////////


    /** The incoming requests. */
    val requests: DataStream[Request] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("requestsTopic", "requests"),
        new SimpleStringSchema(),
        createProperties("requestsAddr", "requestsConsumer"))
        .setStartFromEarliest())
      .flatMap(RequestParser())
      .name("RequestSource")

    /** The coordinator messages. */
    val psMessages: DataStream[HubMessage] = env.addSource(
      new FlinkKafkaConsumer[HubMessage](
        params.get("psMessagesTopic", "psMessages"),
        new TypeInformationSerializationSchema(createTypeInformation[HubMessage], env.getConfig),
        createProperties("psMessagesAddr", "psMessagesConsumer"))
        .setStartFromLatest())
      .name("FeedBackLoopSource")


    ////////////////////////////////////////////// Request Parsing /////////////////////////////////////////////////////


    /** Check the validity of the request */
    val validRequest: DataStream[ControlMessage] = requests
      .keyBy((_: Request) => 0)
      .flatMap(new PipelineMap)
      .setParallelism(1)
      .name("RequestParsing")


    ///////////////////////////////////////////////// Run the Job //////////////////////////////////////////////////////


    runOMLDMJob(env, validRequest, psMessages)


    //////////////////////////////////////////// Execute OML Job ///////////////////////////////////////////////////////


    /** execute program */
    env.execute(params.get("jobName", DefaultJobParameters.defaultJobName))

  }


}
