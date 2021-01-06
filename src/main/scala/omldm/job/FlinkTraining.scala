package omldm.job

import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import omldm.Job.{queryResponse, terminationStats, trainingStats}
import ControlAPI.{JobStatistics, QueryResponse, Statistics}
import mlAPI.math.Point
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.{FlinkHub, FlinkSpoke}
import omldm.utils.{DefaultJobParameters, JobTerminator, PerformanceWriter}
import omldm.utils.KafkaUtils.createProperties
import omldm.utils.generators.MLNodeGenerator
import omldm.utils.partitioners.random_partitioner
import omldm.utils.statistics.StatisticsOperator
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, OutputTag}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector

/**
 * A Flink Job Graph for distributed Machine Learning Training.
 *
 * @param env          The [[StreamExecutionEnvironment]] instance.
 * @param trainingData The training data stream.
 * @param requests     The user requests stream for the Job.
 * @param psMessages   The coordinator messages stream.
 * @param params       The Flink job parameters.
 */
case class FlinkTraining(env: StreamExecutionEnvironment,
                         trainingData: DataStream[Point],
                         requests: DataStream[ControlMessage],
                         psMessages: DataStream[HubMessage])
                        (implicit val params: ParameterTool)
  extends FlinkJob[(DataStream[HubMessage], DataStream[QueryResponse])] {


  //////////////////////////////////////////////// Side Outputs ////////////////////////////////////////////////////////


  val testing: Boolean = params.get("test", DefaultJobParameters.defaultTestParameter).toBoolean
  val jobName: String = params.get("jobName", DefaultJobParameters.defaultJobName)


  ////////////////////////////////////////// Testing Performance Topic /////////////////////////////////////////////////


  /** When the job receives this record then it terminates itself. */
  val jobPerformanceSource: DataStream[_] = env.addSource(
    new FlinkKafkaConsumer[String](params.get("performanceTopic", "performance"),
      new SimpleStringSchema(),
      createProperties("performanceAddr", "performanceConsumer"))
      .setStartFromLatest())
    .flatMap(new JobTerminator(jobName)).name("PerformanceSource")


  /////////////////////////////////////////////////// Training /////////////////////////////////////////////////////////


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

  /** Partitioning the training data along with the control messages to the workers. */
  val trainingDataBlocks: ConnectedStreams[Point, ControlMessage] = trainingData
    .connect(controlMessages)

  /** The parallel learning procedure happens here. */
  val worker: DataStream[SpokeMessage] = trainingDataBlocks
    .process(new FlinkSpoke[MLNodeGenerator](testing))
    .name("FlinkSpoke")

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
      worker.getSideOutput(queryResponse)
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

  override def getJobOutput: (DataStream[HubMessage], DataStream[QueryResponse]) =
    (feedback, worker.getSideOutput(queryResponse).filter(x => x.responseId >= 0))

}
