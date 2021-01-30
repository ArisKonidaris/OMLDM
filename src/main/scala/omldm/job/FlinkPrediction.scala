package omldm.job

import BipartiteTopologyAPI.operations.CallType
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.{DataInstance, Prediction}
import mlAPI.parameters.utils.ParameterDescriptor
import omldm.messages.{ControlMessage, HubMessage, SpokeMessage}
import omldm.operators.FlinkPredictor
import omldm.Job.predictions
import omldm.utils.generators.MLNodeGenerator
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, OutputTag, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector

case class FlinkPrediction(env: StreamExecutionEnvironment,
                           forecastingSource: DataStream[DataInstance],
                           request: DataStream[ControlMessage],
                           updates: DataStream[HubMessage])
                          (implicit val params: ParameterTool)
  extends FlinkJob[DataStream[Prediction]] {

  /** Creating the model updates. */
  val modelUpdates: DataStream[ControlMessage] = updates
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
        override def flatMap(hMessage: HubMessage, collector: Collector[ControlMessage]): Unit = {
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
            case _ =>
          }
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
    .connect(request
      .filter(x => x.getRequest.getRequest != "Query")
      .keyBy(x => x.destination.getNodeId)
      .union(modelUpdates.keyBy(x => x.destination.getNodeId))
    )

  /** The parallel prediction procedure happens here. */
  val predictionStream: DataStream[SpokeMessage] = predictionDataBlocks
    .process(new FlinkPredictor[MLNodeGenerator])
    .name("MLPredictor")

  override def getJobOutput: DataStream[Prediction] = predictionStream.getSideOutput(predictions)

}
