package omldm.operators

import ControlAPI.{DataInstance, Request}
import BipartiteTopologyAPI._
import BipartiteTopologyAPI.network.Node
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import omldm.messages.{ControlMessage, SpokeMessage}
import mlAPI.dataBuffers.DataSet
import omldm.network.FlinkNetwork
import omldm.nodes.spoke.SpokeLogic
import omldm.utils.generators.NodeGenerator
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.reflect.Manifest
import scala.util.Random
import scala.collection.mutable.ListBuffer

class FlinkPredictor[G <: NodeGenerator](implicit man: Manifest[G])
  extends SpokeLogic[DataInstance, ControlMessage, SpokeMessage] {

  /** The nodes trained in the current Flink operator instance. */
  private var nodes: ListState[scala.collection.mutable.Map[Int, BufferingWrapper[DataInstance]]] = _
  private var saved_cache: ListState[DataSet[DataInstance]] = _

  private var collector: Collector[SpokeMessage] = _
  private var context: CoProcessFunction[DataInstance, ControlMessage, SpokeMessage]#Context = _

  Random.setSeed(25)

  /** The process operation of the prediction.
   *
   * The new data point is either forwarded for prediction, or buffered
   * if the operator instance does not have any predictor pipelines.
   *
   * @param data A data point for prediction.
   * @param out  The process operation collector.
   */
  override def processElement1(data: DataInstance,
                               ctx: CoProcessFunction[DataInstance, ControlMessage, SpokeMessage]#Context,
                               out: Collector[SpokeMessage]): Unit = {
    collector = out
    context = ctx
    if (state.nonEmpty) {
      if (cache.nonEmpty) {
        cache.append(data)
        while (cache.nonEmpty) {
          val point = cache.pop.get
          for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
        }
      } else for ((_, node: Node) <- state) node.receiveTuple(Array[Any](data))
    } else cache.append(data)
  }

  /** The process function of the control stream.
   *
   * The control stream is responsible for updating the predictors.
   *
   * @param message The control message with a new model.
   * @param out     The process function collector.
   */
  def processElement2(message: ControlMessage,
                      ctx: CoProcessFunction[DataInstance, ControlMessage, SpokeMessage]#Context,
                      out: Collector[SpokeMessage]): Unit = {
    message match {
      case ControlMessage(network, operation, source, destination, data, request) =>
        checkId(destination.getNodeId)
        collector = out
        context = ctx

        operation match {
          case rpc: RemoteCallIdentifier =>
            if (state.contains(network))
              state(network).receiveMsg(source, rpc, data)

          case null =>
            request match {
              case null => println(s"Empty request in predictor ${getRuntimeContext.getIndexOfThisSubtask}.")
              case req: Request =>
                req.getRequest match {
                  case "Create" =>
                    if (!state.contains(network)) {
                      val hubParallelism: Int = {
                        try {
                          req.getTraining_configuration.getOrDefault("HubParallelism", "1").asInstanceOf[Int]
                        } catch {
                          case _: Throwable => 1
                        }
                      }
                      val flinkNetwork = FlinkNetwork[DataInstance, ControlMessage, SpokeMessage](
                        NodeType.SPOKE,
                        network,
                        getRuntimeContext.getExecutionConfig.getParallelism,
                        hubParallelism)
                      flinkNetwork.setCollector(collector)
                      flinkNetwork.setContext(context)
                      state += (
                        network -> new BufferingWrapper(
                          new NodeId(NodeType.SPOKE, getRuntimeContext.getIndexOfThisSubtask),
                          nodeFactory.generatePredictorNode(req),
                          flinkNetwork).setDefaultOp().asInstanceOf[BufferingWrapper[DataInstance]]
                        )
                    }

                  case "Update" =>

                  case "Query" =>

                  case "Delete" => if (state.contains(network)) state.remove(network)

                  case _: String =>
                    println(s"Invalid request type in predictor ${getRuntimeContext.getIndexOfThisSubtask}.")
                }
            }
        }
    }
  }

  /** Snapshot operation.
   *
   * Takes a snapshot of the operator when
   * a checkpoint has to be performed.
   *
   * @param context Flink's FunctionSnapshotContext.
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    nodes.clear()
    nodes add state
    saved_cache.clear()
    saved_cache add cache
  }

  /** Operator initializer method.
   *
   * Is called every time the user-defined function is initialized,
   * be that when the function is first initialized or be that when
   * the function is actually recovering from an earlier checkpoint.
   *
   * @param context Flink's FunctionSnapshotContext
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {

    nodes = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[scala.collection.mutable.Map[Int, BufferingWrapper[DataInstance]]]("node",
        TypeInformation.of(new TypeHint[scala.collection.mutable.Map[Int, BufferingWrapper[DataInstance]]]() {}))
    )

    saved_cache = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[DataInstance]]("saved_cache",
        TypeInformation.of(new TypeHint[DataSet[DataInstance]]() {}))
    )

    // ============================================ Restart strategy ===================================================

    if (context.isRestored) {

      // ======================================== Restoring the Spokes =================================================
      state.clear()

      var new_state = scala.collection.mutable.Map[Int, BufferingWrapper[DataInstance]]()
      val it_pip = nodes.get.iterator
      val remoteStates = ListBuffer[scala.collection.mutable.Map[Int, BufferingWrapper[DataInstance]]]()
      while (it_pip.hasNext) {
        val next = it_pip.next
        if (next.nonEmpty)
          if (new_state.isEmpty)
            new_state = next
          else {
            assert(new_state.size == next.size)
            remoteStates.append(next)
          }
      }

      for ((key, node) <- state) node.merge((for (remoteState <- remoteStates) yield remoteState(key)).toArray)

      // ====================================== Restoring the data cache ===============================================

      cache.clear()
      cache = mergingDataBuffers(saved_cache)
      if (state.nonEmpty)
        while (cache.nonEmpty) {
          val point = cache.pop.get
          for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
        }
      else
        while (cache.length > cache.getMaxSize)
          cache.pop
      assert(cache.length <= cache.getMaxSize)

    }

  }

  private def checkId(id: Int): Unit = {
    try {
      require(id == getRuntimeContext.getIndexOfThisSubtask,
        s"Flink Predictor ID is not equal to the index of Flink's Subtask.")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def nodeFactory: NodeGenerator = man.runtimeClass.newInstance().asInstanceOf[NodeGenerator]

}