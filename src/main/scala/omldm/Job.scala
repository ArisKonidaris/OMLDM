package omldm

import java.util.Optional
import ControlAPI.{DataInstance, Prediction, QueryResponse, Request}
import mlAPI.math.Point
import omldm.job.{FlinkPrediction, FlinkTraining}
import omldm.messages.{ControlMessage, HubMessage}
import omldm.utils.KafkaUtils.createProperties
import omldm.utils.kafkaPartitioners.FlinkHubMessagePartitioner
import omldm.utils.parsers.dataStream.DataPointParser
import omldm.utils.parsers.requestStream.PipelineMap
import omldm.utils.parsers.{DataInstanceParser, RequestParser}
import omldm.utils.{Checkpointing, DefaultJobParameters}
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import scala.util.Random

object Job {

  def builtTrainingJob(env: StreamExecutionEnvironment,
                       requests: DataStream[ControlMessage],
                       psMessages: DataStream[HubMessage]
                      )(implicit params: ParameterTool)
  : (StreamExecutionEnvironment, DataStream[HubMessage], DataStream[QueryResponse]) = {

    /** The incoming training data. */
    val trainingSource: DataStream[DataInstance] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("trainingDataTopic", "trainingData"),
        new SimpleStringSchema(),
        createProperties("trainingDataAddr", "trainingDataConsumer"))
        .setStartFromEarliest())
      .flatMap(DataInstanceParser())
      .name("TrainingSource")

    /** Parsing the training data */
    val trainingData: DataStream[Point] = trainingSource
      .flatMap(new DataPointParser)
      .name("DataParsing")

    /** Training */
    val distributedTraining = FlinkTraining(env, trainingData, requests, psMessages)
    val (coordinator, responses) = distributedTraining.getJobOutput

    /** The Kafka iteration for emulating parameter server messages */
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

    (env, coordinator, responses)
  }

  def builtPredictionJob(env: StreamExecutionEnvironment,
                         request: DataStream[ControlMessage],
                         updates: DataStream[HubMessage])
                        (implicit params: ParameterTool)
  : (StreamExecutionEnvironment, DataStream[Prediction]) = {

    /** The incoming forecasting data. */
    val forecastingSource: DataStream[DataInstance] = env.addSource(
      new FlinkKafkaConsumer[String](params.get("forecastingDataTopic", "forecastingData"),
        new SimpleStringSchema(),
        createProperties("forecastingDataAddr", "forecastingDataConsumer"))
        .setStartFromEarliest())
      .flatMap(DataInstanceParser())
      .name("ForecastingSource")

    val distributedPrediction = FlinkPrediction(env, forecastingSource, request, updates)

    (env, distributedPrediction.getJobOutput)
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
    val mode = params.get("mode", DefaultJobParameters.mode)


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


    ////////////////////////////////////////////// Building the Job ////////////////////////////////////////////////////


    if (mode.equals("training"))
      builtTrainingJob(env, validRequest, psMessages)
    else if (mode.equals("prediction"))
      builtPredictionJob(env, validRequest, psMessages)
    else
      builtPredictionJob(env, validRequest, builtTrainingJob(env, validRequest, psMessages)._2)

    //////////////////////////////////////////// Execute OML Job ///////////////////////////////////////////////////////


    /** execute program */
    env.execute(params.get("jobName", DefaultJobParameters.defaultJobName))

  }


}
