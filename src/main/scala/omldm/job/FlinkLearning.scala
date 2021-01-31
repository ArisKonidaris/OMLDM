package omldm.job

import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import omldm.Job.{mlNodeSideOutput, terminationStats, trainingStats}
import ControlAPI.{JobStatistics, Prediction, QueryResponse, Statistics}
import mlAPI.math.UsablePoint
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.hub.FlinkHub
import omldm.operators.spoke.FlinkSpoke
import omldm.utils.{DefaultJobParameters, JobTerminator, PerformanceWriter}
import omldm.utils.KafkaUtils.createProperties
import omldm.utils.generators.MLNodeGenerator
import omldm.utils.partitioners.random_partitioner
import omldm.utils.statistics.StatisticsOperator
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

/**
 * A Flink Job Graph for distributed Machine Learning Training.
 *
 * @param env          The [[StreamExecutionEnvironment]] instance.
 * @param dataStream   The training and forecasting data stream.
 * @param requests     The user requests stream for the Job.
 * @param psMessages   The coordinator messages stream.
 * @param params       The Flink job parameters.
 */
case class FlinkLearning(env: StreamExecutionEnvironment,
                         dataStream: DataStream[UsablePoint],
                         requests: DataStream[ControlMessage],
                         psMessages: DataStream[HubMessage])
                        (implicit val params: ParameterTool) {


  //////////////////////////////////////////////// Job Parameters //////////////////////////////////////////////////////


  val testing: Boolean = params.get("test", DefaultJobParameters.defaultTestParameter).toBoolean
  val maxMsgParams: Int = params.get("maxMsgParams", DefaultJobParameters.defaultMaxMsgParams).toInt
  val jobName: String = params.get("jobName", DefaultJobParameters.defaultJobName)


  ////////////////////////////////////////// Testing Performance Topic /////////////////////////////////////////////////


  /** When the job receives this record then it terminates itself. */
  val jobPerformanceSource: DataStream[_] = env.addSource(
    new FlinkKafkaConsumer[String](params.get("performanceTopic", "performance"),
      new SimpleStringSchema(),
      createProperties("performanceAddr", "performanceConsumer"))
      .setStartFromLatest())
    .flatMap(new JobTerminator(jobName)).name("PerformanceSource")


  ///////////////////////////////////////////// Training & Prediction //////////////////////////////////////////////////


  /** The broadcast messages of the Hub. */
  val hubMessages: DataStream[ControlMessage] = psMessages
    .flatMap(new RichFlatMapFunction[HubMessage, ControlMessage] {
      override def flatMap(in: HubMessage, out: Collector[ControlMessage]): Unit = {
        if (in.networkId == -1)
          for (worker <- 0 until getRuntimeContext.getExecutionConfig.getParallelism)
            out.collect(ControlMessage(-1, null, null, new NodeId(NodeType.SPOKE, worker), null, null))
        else
          for ((rpc, dest) <- in.operations zip in.destinations)
            out.collect(ControlMessage(in.getNetworkId, rpc, in.getSource, dest, in.getData, in.getRequest))
      }
    })

  /** Partitioning the Hub's messages along with the request messages to the workers. */
  val controlMessages: DataStream[ControlMessage] = hubMessages
    .partitionCustom(random_partitioner, (x: ControlMessage) => x.destination.getNodeId)
    .union(requests.partitionCustom(random_partitioner, (x: ControlMessage) => x.destination.getNodeId))

  /** Partitioning learning data stream along with the control messages to the workers. */
  val dataBlocks: ConnectedStreams[UsablePoint, ControlMessage] = dataStream.connect(controlMessages)

  /** The parallel learning procedure happens here. */
  val worker: DataStream[SpokeMessage] = dataBlocks
    .process(new FlinkSpoke[MLNodeGenerator](testing, maxMsgParams))
    .name("FlinkSpoke")

  /** The query responses of the spokes. */
  val queryResponses: DataStream[QueryResponse] = worker.getSideOutput(mlNodeSideOutput)
    .filter(x => x.isInstanceOf[QueryResponse])
    .map(x => x.asInstanceOf[QueryResponse])

  /** The predictions of the spokes. */
  val predictions: DataStream[Prediction] = worker.getSideOutput(mlNodeSideOutput)
    .filter(x => x.isInstanceOf[Prediction])
    .map(x => x.asInstanceOf[Prediction])

  /** The coordinator operators, where the learners are merged. */
  val coordinator: DataStream[HubMessage] = worker
    .filter(x => x.networkId != -1)
    .keyBy((x: SpokeMessage) => x.getNetworkId + "_" + x.getDestination.getNodeId)
    .process(new FlinkHub[MLNodeGenerator](testing))
    .name("FlinkHub")

  /** This is activated only when testing the performance of the OMLDM component. */
  val performance: DataStream[JobStatistics] = coordinator.getSideOutput(trainingStats)
    .union(worker.filter(x => x.getNetworkId == -1).map(_ => ("", new Statistics())))
    .union(
      queryResponses
        .filter(x => x.responseId == -1)
        .map(x => (
          "Terminate",
          new Statistics(x.getMlpId, null, x.getCumulativeLoss, 0, 0, 0, x.getDataFitted, x.getScore))
        )
    )
    .keyBy(_ => 0)
    .process(new StatisticsOperator(jobName))

  /** A Kafka Sink for the training performance results. */
  performance
    .map(new PerformanceWriter())
    .addSink(new FlinkKafkaProducer[String](
      params.get("performanceAddr", "localhost:9092"), // broker list
      params.get("performanceTopic", "performance"), // target topic
      new SimpleStringSchema()))
    .name("PerformanceSink")


  val feedback: DataStream[HubMessage] = coordinator.union(performance.getSideOutput(terminationStats))

  def getJobOutput: (DataStream[HubMessage], DataStream[QueryResponse], DataStream[Prediction]) =
    (feedback, queryResponses.filter(x => x.responseId >= 0), predictions)

}
