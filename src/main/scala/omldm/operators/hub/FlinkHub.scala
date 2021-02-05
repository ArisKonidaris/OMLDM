package omldm.operators.hub

import BipartiteTopologyAPI.GenericWrapper
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.{Request, Statistics}
import mlAPI.mlParameterServers.MLParameterServer
import mlAPI.mlParameterServers.proto.CentralizedMLServer
import omldm.Job.trainingStats
import omldm.messages.{HubMessage, SpokeMessage}
import omldm.network.FlinkNetwork
import omldm.state.{DataAggregateFunction, NodeAccumulator, NodeAggregateFunction, SpokeMessageAccumulator}
import omldm.utils.generators.NodeGenerator
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
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
            if (state.get == null)
              cache add workerMessage
            else {
              breakable {
                while (true) {
                  cache.get match {
                    case Some(mess: SpokeMessage) =>
                      state add(mess, ctx, out)
                      state.get().getNode match {
                        case ps: MLParameterServer[_, _] => println("Status: " + ps.getNumberOfFittedData)
                        case cl: CentralizedMLServer => println("Status: " + cl.fitted)
                      }
//                      println("Status: " + state.get().getNode.asInstanceOf[MLParameterServer[_, _]].getNumberOfFittedData)
                    case _ => break
                  }
                }
              }
              state add(workerMessage, ctx, out)
              if (test) {
                state.get().getNode match {
                  case ps: MLParameterServer[_, _] => println("Status: " + ps.getNumberOfFittedData)
                  case cl: CentralizedMLServer => println("Status: " + cl.fitted)
                }
//                println("Status: " + state.get().getNode.asInstanceOf[MLParameterServer[_, _]].getNumberOfFittedData)
//                println(state.get().getNode.asInstanceOf[MLParameterServer[_, _]].getProtocolStatistics)
                val mlpId: String = state.get().getNetwork.describe().getNetworkId + "_" + ctx.getCurrentKey
                val mlpStats = {
                  state.get().getNode match {
                    case ps: MLParameterServer[_, _] =>
                      val s = ps.getProtocolStatistics
                      new Statistics(
                        mlpId.split("_")(0).toInt,
                        s.getProtocol,
                        s.getModelsShipped,
                        s.getBytesShipped,
                        s.getNumOfBlocks,
                        state.get().getNode.asInstanceOf[MLParameterServer[_, _]].getNumberOfFittedData
                      )
                    case cl: CentralizedMLServer =>
                      val s = cl.getProtocolStatistics
                      new Statistics(
                        mlpId.split("_")(0).toInt,
                        s.getProtocol,
                        s.getModelsShipped,
                        s.getBytesShipped,
                        s.getNumOfBlocks,
                        cl.fitted
                      )
                  }
                }
                ctx.output(trainingStats, (mlpId, mlpStats))
              }
            }
        }
    }

  }

  private def nodeFactory: NodeGenerator = man.runtimeClass.newInstance().asInstanceOf[NodeGenerator]

  private def generateHub(message: SpokeMessage,
                          ctx: KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context,
                          out: Collector[HubMessage]): Unit = {
    val request: Request = message.getRequest
    val hubId: NodeId = new NodeId(NodeType.HUB, message.getDestination.getNodeId)
    val parallelTraining: Boolean = parallelism > 1
    val flinkNetwork = FlinkNetwork[SpokeMessage, HubMessage, HubMessage](
      NodeType.HUB,
      message.getNetworkId,
      parallelism,
      if (parallelTraining && request.getTrainingConfiguration.containsKey("HubParallelism")) {
        mlAPI.utils.Parsing.IntegerParsing(request.getTrainingConfiguration.asScala,"HubParallelism", 1)
      } else
        1
    )

    val genWrapper = new GenericWrapper(
      hubId,
      {
        if (parallelTraining)
          nodeFactory.generateHubNode(request)
        else {
          request.getTrainingConfiguration.replace("protocol", "CentralizedTraining".asInstanceOf[AnyRef])
          nodeFactory.generateHubNode(request)
        }
      },
      flinkNetwork
    )
    state add(SpokeMessage(0, null, null, null, genWrapper, null), ctx, out)
  }

}

