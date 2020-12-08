package omldm.operators

import BipartiteTopologyAPI.GenericWrapper
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.Request
import omldm.OMLDM_Job.trainingTime
import omldm.messages.{HubMessage, SpokeMessage}
import omldm.network.FlinkNetwork
import omldm.nodes.hub.HubLogic
import omldm.state.{DataAggregateFunction, NodeAccumulator, NodeAggregateFunction, SpokeMessageAccumulator}
import omldm.utils.generators.NodeGenerator
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.reflect.Manifest
import scala.util.control.Breaks.{break, breakable}

class FlinkHub[G <: NodeGenerator](val test: Boolean)(implicit man: Manifest[G])
  extends HubLogic[SpokeMessage, HubMessage] {

  override protected var state: AggregatingState
    [
    (SpokeMessage, KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context, Collector[HubMessage]),
    GenericWrapper
    ] = _

  override protected var cache: AggregatingState[SpokeMessage, Option[SpokeMessage]] = _

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getAggregatingState[
      (SpokeMessage, KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context, Collector[HubMessage]),
      NodeAccumulator, GenericWrapper
    ](
      new AggregatingStateDescriptor(
        "state",
        new NodeAggregateFunction(),
        createTypeInformation[NodeAccumulator]))

    cache = getRuntimeContext.getAggregatingState[SpokeMessage, SpokeMessageAccumulator, Option[SpokeMessage]](
      new AggregatingStateDescriptor(
        "cache",
        new DataAggregateFunction(),
        createTypeInformation[SpokeMessageAccumulator]))

  }

  override def processElement(workerMessage: SpokeMessage,
                              ctx: KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context,
                              out: Collector[HubMessage]): Unit = {

    workerMessage match {
      case SpokeMessage(network, operation, source, destination, data, request) =>
        request match {
          case req: Request =>
            req.getRequest match {
              case "Create" => generateHub(workerMessage, ctx, out)
              case "Update" =>
              case "Query" =>
              case "Delete" => state.clear()
              case _: String =>
                throw new RuntimeException(s"Unsupported request on Hub ${network + "_" + ctx.getCurrentKey}.")
            }
          case null =>
            if (state.get == null) {
              cache add workerMessage
            } else {
              if(test) ctx.output(trainingTime, ctx.timestamp())
              breakable {
                while(true) {
                  cache.get match {
                    case Some(mess: SpokeMessage) => state add (mess, ctx, out)
                    case _ => break
                  }
                }
              }
              state add (workerMessage, ctx, out)
            }
        }
    }

  }

  private def nodeFactory: NodeGenerator = man.runtimeClass.newInstance().asInstanceOf[NodeGenerator]

  private def generateHub(message: SpokeMessage,
                          ctx: KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context,
                          out: Collector[HubMessage]): Unit = {
    val request: Request = message.getRequest
    val networkId: Int = message.getNetworkId
    val hubId: NodeId = new NodeId(NodeType.HUB, message.getDestination.getNodeId)
    val flinkNetwork = FlinkNetwork[SpokeMessage, HubMessage, HubMessage](
      NodeType.HUB,
      networkId,
      getRuntimeContext.getExecutionConfig.getParallelism,
      if (request.getTraining_configuration.containsKey("HubParallelism"))
        request.getTraining_configuration.get("HubParallelism").asInstanceOf[Double].toInt
      else 1
    )

    val genWrapper = new GenericWrapper(hubId, nodeFactory.generateHubNode(request), flinkNetwork)
    state add (SpokeMessage(0, null, null, null, genWrapper, null), ctx, out)

  }

}

