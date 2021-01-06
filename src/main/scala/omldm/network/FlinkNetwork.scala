package omldm.network

import java.io.Serializable
import BipartiteTopologyAPI.network.Network
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NetworkDescriptor, NodeId, NodeType}
import ControlAPI._
import omldm.Job.{queryResponse, predictions}
import omldm.messages.{HubMessage, SpokeMessage}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

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
              case qResp: QueryResponse =>
                if (qResp.getResponseId == -1)
                  context.output(queryResponse, qResp)
                else {
                  val maxParamBucketSize = 10000
                  val bucketedPar = mutable.HashMap[Int, util.Map[String, AnyRef]]()
                  qResp.learner.getParameters.entrySet().forEach(
                    entry => {
                      val paramsName: String = entry.getKey
                      val params: Array[Double] = {
                        entry.getValue.asInstanceOf[Any] match {
                          case double: Double => Array(double)
                          case doubles: Array[Double] => doubles
                          case null => return
                        }
                      }
                      val buckets: Int = params.length / maxParamBucketSize +
                        {
                          if (params.length % maxParamBucketSize == 0) 0 else 1
                        }
                      if (buckets == 1) {
                        val key = paramsName
                        val value = { if (params.length == 1) params.head else params }.asInstanceOf[AnyRef]
                        if (!bucketedPar.contains(0))
                          bucketedPar.put(0, {
                            val m = new util.HashMap[String, AnyRef]()
                            m.put(key, value)
                            m
                          })
                        else
                          bucketedPar(0).put(key, value)
                      } else {
                        for (i: Int <- 0 until buckets) {
                          val start = i * maxParamBucketSize
                          val end = {
                            if (i == buckets - 1)
                              start + params.length % maxParamBucketSize - 1
                            else
                              (i + 1) * maxParamBucketSize - 1
                          }
                          val key = paramsName + "[" + start + "-" + end + "]"
                          val value = {
                            val slice = params.slice(start, end + 1)
                            (if (slice.length == 1) slice.head else slice).asInstanceOf[AnyRef]
                          }
                          if (!bucketedPar.contains(i))
                            bucketedPar.put(i, {
                              val m = new util.HashMap[String, AnyRef]()
                              m.put(key, value)
                              m
                            })
                          else
                            bucketedPar(i).put(key, value)
                        }
                      }
                    }
                  )
                  if (bucketedPar.size == 1)
                    context.output(queryResponse, qResp)
                  else {
                    for ((key, value) <- bucketedPar.toArray.sortBy(x => x._1)) {
                      val bucketResponse = {
                        if (key == 0) {
                          new QueryResponse(qResp.getResponseId, 0, qResp.getMlpId,
                            null,
                            {
                              val learner = qResp.getLearner
                              learner.setDataStructure(null)
                              learner.setHyperParameters(null)
                              learner.setParameters(value)
                              learner
                            }, null, null, null, null, null
                          )
                        } else {
                          new QueryResponse(qResp.getResponseId,
                            key,
                            qResp.getMlpId,
                            if (key == bucketedPar.size -1) qResp.getPreprocessors else null,
                            {
                              val learner = qResp.getLearner
                              if (key != bucketedPar.size -1) {
                                learner.setDataStructure(null)
                                learner.setHyperParameters(null)
                              }
                              learner.setParameters(value)
                              learner
                            },
                            qResp.getProtocol,
                            qResp.getDataFitted,
                            qResp.getLoss,
                            qResp.getCumulativeLoss,
                            qResp.getScore
                          )
                        }
                      }
                      context.output(queryResponse, bucketResponse)
                    }
                  }
                }
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
