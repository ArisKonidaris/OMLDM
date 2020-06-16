package omldm.network

import java.io.Serializable

import BipartiteTopologyAPI.network.Network
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NetworkDescriptor, NodeId, NodeType}
import ControlAPI._
import omldm.messages.{HubMessage, SpokeMessage}
import omldm.OML_Job.{predictions, queryResponse}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

case class FlinkNetwork[InMsg <: Serializable, CtrlMsg <: Serializable, OutMsg <: Serializable]
(private var nodeType: NodeType,
 private var networkId: Int,
 private var numberOfSpokes: Int,
 private var numberOfHubs: Int) extends Network {

  private var collector: Collector[OutMsg] = _
  private var context: CoProcessFunction[InMsg, CtrlMsg, OutMsg]#Context = _
  private var keyedContext: KeyedProcessFunction[String, InMsg, OutMsg]#Context = _

  def setNodeType(nodeType: NodeType): Unit = this.nodeType = nodeType

  def setNetworkID(networkId: Int): Unit = this.networkId = networkId

  def setNumberOfSpokes(numberOfSpokes: Int): Unit = this.numberOfSpokes = numberOfSpokes

  def setNumberOfHubs(numberOfHubs: Int): Unit = this.numberOfHubs = numberOfHubs

  def setCollector(collector: Collector[OutMsg]): Unit = this.collector = collector

  def setContext(context: CoProcessFunction[InMsg, CtrlMsg, OutMsg]#Context): Unit = {
    this.context = context
  }

  def setKeyedContext(keyedContext: KeyedProcessFunction[String, InMsg, OutMsg]#Context): Unit = {
    this.keyedContext = keyedContext
  }

  override def send(source: NodeId, destination: NodeId, rpc: RemoteCallIdentifier, message: Serializable): Unit = {
    destination match {
      case null =>
        nodeType match {
          case NodeType.SPOKE =>
            message.asInstanceOf[Array[Any]](0) match {
              case qResp: QueryResponse => context.output(queryResponse, qResp)
              case pred: Prediction => context.output(predictions, pred)
            }
          case NodeType.HUB =>
            message.asInstanceOf[Array[Any]](0) match {
              case qResp: QueryResponse => keyedContext.output(queryResponse, qResp)
              case pred: Prediction => keyedContext.output(predictions, pred)
            }
        }
      case dest: NodeId =>
        nodeType match {
          case NodeType.SPOKE =>
            collector.collect(SpokeMessage(networkId, rpc, source, dest, message, null).asInstanceOf[OutMsg])
          case NodeType.HUB =>
            collector.collect(
              HubMessage(networkId,
                Array(rpc),
                source,
                Array(dest),
                message,
                null
              ).asInstanceOf[OutMsg]
            )
        }
    }
  }

  override def broadcast(source: NodeId, rpcs: java.util.Map[NodeId, RemoteCallIdentifier], message: Serializable)
  : Unit = {
    nodeType match {
      case NodeType.SPOKE =>
        for ((dest, rpc) <- rpcs.asScala)
          collector.collect(SpokeMessage(networkId, rpc, source, dest, message, null).asInstanceOf[OutMsg])
      case NodeType.HUB =>
        val destinations: ListBuffer[NodeId] = ListBuffer()
        val operations: ListBuffer[RemoteCallIdentifier] = ListBuffer()
         for ((dest,rpc) <- rpcs.asScala){
           destinations.append(dest)
           operations.append(rpc)
         }
        collector.collect(
          HubMessage(networkId, operations.toArray, source, destinations.toArray,message, null).asInstanceOf[OutMsg]
        )
    }
  }

  override def describe(): NetworkDescriptor = new NetworkDescriptor(networkId, numberOfSpokes, numberOfHubs)

}
