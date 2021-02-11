package omldm.network

import java.io.Serializable
import BipartiteTopologyAPI.interfaces.Network
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NetworkDescriptor, NodeId, NodeType}
import ControlAPI._
import mlAPI.protocols.IntWrapper
import omldm.Job.{hubSideOutput, spokeSideOutput}
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
 private var numberOfSpokes: IntWrapper,
 private var numberOfHubs: Int) extends Network {

  private var collector: Collector[OutMsg] = _
  private var context: CoProcessFunction[InMsg, CtrlMsg, OutMsg]#Context = _
  private var keyedContext: KeyedProcessFunction[String, InMsg, OutMsg]#Context = _

  def setNodeType(nodeType: NodeType): Unit = this.nodeType = nodeType

  def setNetworkID(networkId: Int): Unit = this.networkId = networkId

  def setNumberOfSpokes(numberOfSpokes: IntWrapper): Unit = this.numberOfSpokes = numberOfSpokes

  def setNumberOfHubs(numberOfHubs: Int): Unit = this.numberOfHubs = numberOfHubs

  def setCollector(collector: Collector[OutMsg]): Unit = this.collector = collector

  def setContext(context: CoProcessFunction[InMsg, CtrlMsg, OutMsg]#Context): Unit = {
    this.context = context
  }

  def setKeyedContext(keyedContext: KeyedProcessFunction[String, InMsg, OutMsg]#Context): Unit = {
    this.keyedContext = keyedContext
  }

  def split(entrySet: util.Set[util.Map.Entry[String, AnyRef]])
  : mutable.HashMap[Int, util.Map[String, AnyRef]] = {
    val maxParamBucketSize = 10000
    val bucketedPar = mutable.HashMap[Int, util.Map[String, AnyRef]]()
    entrySet.forEach(
      entry => {
        val paramsName: String = entry.getKey
        entry.getValue match {
          case params: String =>
            val buckets: Int = params.length / maxParamBucketSize + {
              if (params.length % maxParamBucketSize == 0) 0 else 1
            }
            if (buckets == 1) {
              val key = paramsName
              val value = {
                if (params.length == 1) params.head else params
              }.asInstanceOf[AnyRef]
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
          case _ =>
            val params: Array[_] = {
              entry.getValue match {
                case null => Array()
                case values: Array[_] => values
                case value: Any => Array(value)
              }
            }
            val buckets: Int = params.length / maxParamBucketSize + {
              if (params.length % maxParamBucketSize == 0) 0 else 1
            }
            if (buckets == 1) {
              val key = paramsName
              val value = {
                if (params.length == 1) params.head else params
              }.asInstanceOf[AnyRef]
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
      }
    )
    bucketedPar
  }

  def sendQueryResponse(qResp: QueryResponse, nodeType: NodeType): Unit = {
    if (qResp.getResponseId == -1)
      nodeType match {
        case NodeType.SPOKE => context.output(spokeSideOutput, qResp)
        case NodeType.HUB => keyedContext.output(hubSideOutput, qResp)
      }
    else {
      val bucketedPar: mutable.HashMap[Int, util.Map[String, AnyRef]] = {
        if (qResp.learner.getParameters != null)
          split(qResp.learner.getParameters.entrySet())
        else
          new mutable.HashMap[Int, util.Map[String, AnyRef]]()
      }
      val bucketedHyperPar: mutable.HashMap[Int, util.Map[String, AnyRef]] = {
        if (qResp.learner.hyperParameters != null)
          split(qResp.learner.hyperParameters.entrySet())
        else
          new mutable.HashMap[Int, util.Map[String, AnyRef]]()
      }
      val bucketedStr: mutable.HashMap[Int, util.Map[String, AnyRef]] = {
        if (qResp.learner.dataStructure != null)
          split(qResp.learner.dataStructure.entrySet())
        else
          new mutable.HashMap[Int, util.Map[String, AnyRef]]()
      }
      val maxBuckets: Int = scala.math.max(scala.math.max(bucketedPar.size, bucketedHyperPar.size), bucketedStr.size) - 1
      val mixedMap = mutable.HashMap[Int, (util.Map[String, AnyRef], util.Map[String, AnyRef], util.Map[String, AnyRef])]()
      for (i <- 0 to maxBuckets) {
        val item1 = if (bucketedPar.contains(i)) bucketedPar(i) else null
        val item2 = if (bucketedHyperPar.contains(i)) bucketedHyperPar(i) else null
        val item3 = if (bucketedStr.contains(i)) bucketedStr(i) else null
        mixedMap.put(i, (item1, item2, item3))
      }
      if (maxBuckets == 0)
        nodeType match {
          case NodeType.SPOKE => context.output(spokeSideOutput, qResp)
          case NodeType.HUB => keyedContext.output(hubSideOutput, qResp)
        }
      else {
        for ((key, value) <- mixedMap.toArray.sortBy(x => x._1)) {
          val bucketResponse = {
            if (key == 0) {
              new QueryResponse(
                qResp.getResponseId,
                0,
                qResp.getMlpId,
                null,
                {
                  val learner = qResp.getLearner
                  learner.setDataStructure(value._3)
                  learner.setHyperParameters(value._2)
                  learner.setParameters(value._1)
                  learner
                }, null, null, null, null, null
              )
            } else {
              new QueryResponse(
                qResp.getResponseId,
                key,
                qResp.getMlpId,
                if (key == bucketedPar.size -1) qResp.getPreprocessors else null,
                {
                  val learner = qResp.getLearner
                  if (key != bucketedPar.size -1) {
                    learner.setDataStructure(value._3)
                    learner.setHyperParameters(value._2)
                  }
                  learner.setParameters(value._1)
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
          nodeType match {
            case NodeType.SPOKE => context.output(spokeSideOutput, bucketResponse)
            case NodeType.HUB => keyedContext.output(hubSideOutput, bucketResponse)
          }
        }
      }
    }
  }

//  def sendQueryResponse(qResp: QueryResponse, nodeType: NodeType): Unit = {
//    if (qResp.getResponseId == -1)
//      nodeType match {
//        case NodeType.SPOKE => context.output(spokeSideOutput, qResp)
//        case NodeType.HUB => keyedContext.output(hubSideOutput, qResp)
//      }
//    else {
//      if (qResp.learner.getName == "HT") {
//        nodeType match {
//          case NodeType.SPOKE => context.output(spokeSideOutput, qResp)
//          case NodeType.HUB => keyedContext.output(hubSideOutput, qResp)
//        }
//      } else {
//        val maxParamBucketSize = 10000
//        val bucketedPar = mutable.HashMap[Int, util.Map[String, AnyRef]]()
//        qResp.learner.getParameters.entrySet().forEach(
//          entry => {
//            val paramsName: String = entry.getKey
//            val params: Array[Double] = {
//              entry.getValue.asInstanceOf[Any] match {
//                case double: Double => Array(double)
//                case doubles: Array[Double] => doubles
//                case null => Array()
//              }
//            }
//            val buckets: Int = params.length / maxParamBucketSize +
//              {
//                if (params.length % maxParamBucketSize == 0) 0 else 1
//              }
//            if (buckets == 1) {
//              val key = paramsName
//              val value = { if (params.length == 1) params.head else params }.asInstanceOf[AnyRef]
//              if (!bucketedPar.contains(0))
//                bucketedPar.put(0, {
//                  val m = new util.HashMap[String, AnyRef]()
//                  m.put(key, value)
//                  m
//                })
//              else
//                bucketedPar(0).put(key, value)
//            } else {
//              for (i: Int <- 0 until buckets) {
//                val start = i * maxParamBucketSize
//                val end = {
//                  if (i == buckets - 1)
//                    start + params.length % maxParamBucketSize - 1
//                  else
//                    (i + 1) * maxParamBucketSize - 1
//                }
//                val key = paramsName + "[" + start + "-" + end + "]"
//                val value = {
//                  val slice = params.slice(start, end + 1)
//                  (if (slice.length == 1) slice.head else slice).asInstanceOf[AnyRef]
//                }
//                if (!bucketedPar.contains(i))
//                  bucketedPar.put(i, {
//                    val m = new util.HashMap[String, AnyRef]()
//                    m.put(key, value)
//                    m
//                  })
//                else
//                  bucketedPar(i).put(key, value)
//              }
//            }
//          }
//        )
//        if (bucketedPar.size == 1)
//          nodeType match {
//            case NodeType.SPOKE => context.output(spokeSideOutput, qResp)
//            case NodeType.HUB => keyedContext.output(hubSideOutput, qResp)
//          }
//        else {
//          for ((key, value) <- bucketedPar.toArray.sortBy(x => x._1)) {
//            val bucketResponse = {
//              if (key == 0) {
//                new QueryResponse(qResp.getResponseId, 0, qResp.getMlpId,
//                  null,
//                  {
//                    val learner = qResp.getLearner
//                    learner.setDataStructure(null)
//                    learner.setHyperParameters(null)
//                    learner.setParameters(value)
//                    learner
//                  }, null, null, null, null, null
//                )
//              } else {
//                new QueryResponse(qResp.getResponseId,
//                  key,
//                  qResp.getMlpId,
//                  if (key == bucketedPar.size -1) qResp.getPreprocessors else null,
//                  {
//                    val learner = qResp.getLearner
//                    if (key != bucketedPar.size -1) {
//                      learner.setDataStructure(null)
//                      learner.setHyperParameters(null)
//                    }
//                    learner.setParameters(value)
//                    learner
//                  },
//                  qResp.getProtocol,
//                  qResp.getDataFitted,
//                  qResp.getLoss,
//                  qResp.getCumulativeLoss,
//                  qResp.getScore
//                )
//              }
//            }
//            nodeType match {
//              case NodeType.SPOKE => context.output(spokeSideOutput, bucketResponse)
//              case NodeType.HUB => keyedContext.output(hubSideOutput, bucketResponse)
//            }
//          }
//        }
//      }
//    }
//  }

  override def send(source: NodeId, destination: NodeId, rpc: RemoteCallIdentifier, message: Serializable): Unit = {
    destination match {
      case null =>
        nodeType match {
          case NodeType.SPOKE =>
            message.asInstanceOf[Array[Any]](0) match {
              case qResp: QueryResponse => sendQueryResponse(qResp, NodeType.SPOKE)
              case pred: Prediction => context.output(spokeSideOutput, pred)
            }
          case NodeType.HUB =>
            message.asInstanceOf[Array[Any]](0) match {
              case qResp: QueryResponse => sendQueryResponse(qResp, NodeType.HUB)
              case pred: Prediction => keyedContext.output(hubSideOutput, pred)
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

  override def describe(): NetworkDescriptor = new NetworkDescriptor(networkId, numberOfSpokes.getInt, numberOfHubs)

}
