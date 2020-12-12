package omldm.job

import java.sql.Timestamp

import ControlAPI.QueryResponse
import mlAPI.math.Point
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.{FlinkHub, FlinkSpoke}
import omldm.utils.DefaultJobParameters
import omldm.utils.generators.MLNodeGenerator
import omldm.utils.partitioners.random_partitioner
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, OutputTag}
import org.apache.flink.streaming.api.scala.{createTypeInformation, StreamExecutionEnvironment}
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
  extends Flink_Job[(DataStream[HubMessage], DataStream[QueryResponse])] {


  //////////////////////////////////////////////// Side Outputs ////////////////////////////////////////////////////////


  val trainingTime: OutputTag[Any] = OutputTag[Any]("trainingTime")
  val queryResponse: OutputTag[QueryResponse] = OutputTag[QueryResponse]("QueryResponse")


  /////////////////////////////////////////////////// Training /////////////////////////////////////////////////////////


  /** The broadcast messages of the Hub. */
  val hubMessages: DataStream[ControlMessage] = psMessages
    .flatMap(new RichFlatMapFunction[HubMessage, ControlMessage] {
      override def flatMap(in: HubMessage, out: Collector[ControlMessage]): Unit = {
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
    .process(new FlinkSpoke[MLNodeGenerator])
    .name("FlinkSpoke")

  /** The coordinator operators, where the learners are merged. */
  val coordinator: DataStream[HubMessage] = worker
    .keyBy((x: SpokeMessage) => x.getNetworkId + "_" + x.getDestination.getNodeId)
    .process(new FlinkHub[MLNodeGenerator](params.get("test", DefaultJobParameters.defaultTestParameter).toBoolean))
    .name("FlinkHub")

  /** This is activated only when testing the performance of the OMLDM component. */
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
        if (start.value() == null) {
          println("Starting test at time: " + new Timestamp(messageTimestamp.asInstanceOf[Long]).getTime)
//          start.update(messageTimestamp.asInstanceOf[Long])
          start.update(ctx.timestamp())
        } else {
//          end.update(messageTimestamp.asInstanceOf[Long])
          end.update(ctx.timestamp())
        }

        // Set the state's timestamp to the record's assigned timestamp.
        val tempTime = ctx.timestamp()
        timestampState.update(tempTime)

        // Schedule the next timer timeout ms from the current record time.
        ctx.timerService.registerEventTimeTimer(tempTime + timeout)

      }

      override def onTimer(timestamp: Long,
                           ctx: KeyedProcessFunction[Int, Any, Long]#OnTimerContext, out: Collector[Long])
      : Unit = {
        // Check if this is an outdated timer or the latest timer.
        if (timestamp == timestampState.value + timeout)
          throw new Exception("End of Job.\nTest time: " + (end.value() - start.value()))
      }

    })

  override def getJobOutput: (DataStream[HubMessage], DataStream[QueryResponse]) =
    (coordinator, worker.getSideOutput(queryResponse))

}
