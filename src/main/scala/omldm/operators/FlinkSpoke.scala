package omldm.operators

import java.io.Serializable
import BipartiteTopologyAPI.BufferingWrapper
import BipartiteTopologyAPI.network.Node
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.Request
import mlAPI.dataBuffers.DataSet
import mlAPI.math.Point
import omldm.messages.{ControlMessage, SpokeMessage}
import omldm.network.FlinkNetwork
import omldm.nodes.spoke.SpokeLogic
import omldm.utils.generators.NodeGenerator
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.reflect.Manifest
import scala.util.Random

/** A CoFlatMap Flink Function modelling a worker request in a star distributed topology. */
class FlinkSpoke[G <: NodeGenerator](val test: Boolean)(implicit man: Manifest[G])
  extends SpokeLogic[Point, ControlMessage, SpokeMessage] {

  /** A counter used to sample data points for testing the score of the model. */
  private var count: Int = 0

  /** A counter used for testing the performance of the training job. */
  private var testingCount: Int = 0

  /** The test set buffer. */
  private var testSet: DataSet[Point] = new DataSet[Point](500)
  private var savedTestSet: ListState[DataSet[Point]] = _

  /** The nodes trained in the current Flink operator instance. */
  private var nodes: ListState[scala.collection.mutable.Map[Int, BufferingWrapper[Point]]] = _
  private var savedCache: ListState[DataSet[Point]] = _

  private var collector: Collector[SpokeMessage] = _
  private var context: CoProcessFunction[Point, ControlMessage, SpokeMessage]#Context = _

  Random.setSeed(25)

  /** The process for the fitting phase of the learners.
   *
   * The new data point is either fitted directly to the learner, buffered if
   * the workers waits for the response of the parameter server or used as a
   * test point for testing the performance of the model.
   *
   * @param data A data point for training.
   * @param out  The process collector.
   */
  override def processElement1(data: Point,
                               ctx: CoProcessFunction[Point, ControlMessage, SpokeMessage]#Context,
                               out: Collector[SpokeMessage]): Unit = {
    collector = out
    context = ctx
    if (state.nonEmpty) {
      if (cache.nonEmpty) {
        cache.append(data)
        while (cache.nonEmpty) handleData(cache.pop.get)
      } else handleData(data)
    } else cache.append(data)

    // For calculating the performance of the training procedure.
    if (test) {
      testingCount += 1
      if (testingCount == 100) {
        testingCount = 0
        out.collect(SpokeMessage(-1, null, null, null, null, null))
      }
    }
  }

  private def handleData(data: Serializable): Unit = {
    // Train or test point.
    if (getRuntimeContext.getIndexOfThisSubtask == 0) {
      if (count >= 8) {
        testSet.append(data.asInstanceOf[Point]) match {
          case Some(point: Serializable) => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
          case None =>
        }
      } else for ((_, node: Node) <- state) node.receiveTuple(Array[Any](data))
      count += 1
      if (count == 10) count = 0
    } else {
      if (testSet.nonEmpty) {
        testSet.append(data.asInstanceOf[Point]) match {
          case Some(point: Serializable) => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
          case None =>
        }
        while (testSet.nonEmpty) {
          val point = testSet.pop.get
          for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
        }
      } else for ((_, node: Node) <- state) node.receiveTuple(Array[Any](data))
    }
  }

  /** The process function of the control stream.
   *
   * The control stream are the parameter server messages
   * and the User's control mechanisms.
   *
   * @param message The control message
   * @param out     The process function collector
   */
  def processElement2(message: ControlMessage,
                      ctx: CoProcessFunction[Point, ControlMessage, SpokeMessage]#Context,
                      out: Collector[SpokeMessage]): Unit = {
    message match {
      case ControlMessage(network, operation, source, destination, data, request) =>
        checkId(destination.getNodeId)
        collector = out
        context = ctx

        operation match {
          case rpc: RemoteCallIdentifier =>
            if (state.contains(network)) {
              state(network).receiveMsg(source, rpc, data)
              for ((net: Int, node: Node) <- state) if (net != network) node.toggle()
            }

          case null =>
            request match {
              case null =>
                if (network == -1)
                  for ((_, node: Node) <- state)
                    node.receiveQuery(-1, (node.getMeanBufferSize / (parallelism * 1.0), testSet.dataBuffer.toArray))
                else
                  println(s"Empty request in worker ${getRuntimeContext.getIndexOfThisSubtask}.")
              case req: Request =>
                req.getRequest match {
                  case "Create" =>
                    if (!state.contains(network)) {
                      val parallelTraining: Boolean = parallelism > 1
                      val hubParallelism: Int = {
                        if (parallelTraining)
                          try {
                            req.getTrainingConfiguration.getOrDefault("HubParallelism", "1").asInstanceOf[Int]
                          } catch {
                            case _: Throwable => 1
                          }
                        else
                          1
                      }
                      val flinkNetwork = FlinkNetwork[Point, ControlMessage, SpokeMessage](
                        NodeType.SPOKE,
                        network,
                        parallelism,
                        hubParallelism)
                      flinkNetwork.setCollector(collector)
                      flinkNetwork.setContext(context)
                      state += (
                        network -> new BufferingWrapper(
                          new NodeId(NodeType.SPOKE, getRuntimeContext.getIndexOfThisSubtask),
                          {
                            if (parallelTraining)
                              nodeFactory.generateSpokeNode(req)
                            else {
                              req.getTrainingConfiguration.replace("protocol", "CentralizedTraining".asInstanceOf[AnyRef])
                              nodeFactory.generateSpokeNode(req)
                            }
                          },
                          flinkNetwork)
                        )
                      if (getRuntimeContext.getIndexOfThisSubtask == 0)
                        for (i <- 0 until hubParallelism)
                          out.collect(SpokeMessage(network, null, null, new NodeId(NodeType.HUB, i), null, req))
                    }

                  case "Update" =>

                  case "Query" =>
                    if (req.getRequestId != null)
                      if (state.contains(network))
                        state(network).receiveQuery(req.getRequestId, (0, testSet.dataBuffer.toArray))
                      else
                        println("No such Network.")
                    else println("No requestId given for the query.")

                  case "Delete" => if (state.contains(network)) state.remove(network)

                  case _: String =>
                    println(s"Invalid req type in worker ${getRuntimeContext.getIndexOfThisSubtask}.")
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

    // ======================================== Snapshot the test set ==================================================

    if (testSet != null) {
      savedTestSet.clear()
      savedTestSet add testSet
    }

    // ================================ Snapshot the network nodes and the cache =======================================

    nodes.clear()
    nodes add state
    savedCache.clear()
    savedCache add cache

  }

  /** Operator initializer method.
   *
   * Is called every time the user-defined function is initialized,
   * be that when the function is first initialized or be that when
   * the function is actually recovering from an earlier checkpoint.
   *
   * @param context Flink's FunctionSnapshotContext.
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {

    nodes = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[scala.collection.mutable.Map[Int, BufferingWrapper[Point]]]("node",
        TypeInformation.of(new TypeHint[scala.collection.mutable.Map[Int, BufferingWrapper[Point]]]() {}))
    )

    savedTestSet = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[Point]]("savedTestSet",
        TypeInformation.of(new TypeHint[DataSet[Point]]() {}))
    )

    savedCache = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[Point]]("savedCache",
        TypeInformation.of(new TypeHint[DataSet[Point]]() {}))
    )

    // ============================================ Restart strategy ===================================================

    if (context.isRestored) {

      // ======================================== Restoring the Spokes =================================================

      state.clear()

      var new_state = scala.collection.mutable.Map[Int, BufferingWrapper[Point]]()
      val it_pip = nodes.get.iterator
      val remoteStates = ListBuffer[scala.collection.mutable.Map[Int, BufferingWrapper[Point]]]()
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

      // ====================================== Restoring the test set =================================================

      testSet.clear()
      testSet = mergingDataBuffers(savedTestSet)
      if (state.nonEmpty)
        while (testSet.length > testSet.getMaxSize)
          for ((_, node) <- state)
            node.receiveTuple(testSet.pop.get)
      else
        while (testSet.length > testSet.getMaxSize)
          testSet.pop
      assert(testSet.length <= testSet.getMaxSize)

      // ====================================== Restoring the data cache ===============================================

      cache.clear()
      cache = mergingDataBuffers(savedCache)
      if (state.nonEmpty)
        while (cache.nonEmpty)
          handleData(cache.pop.get)
      else
        while (cache.length > cache.getMaxSize)
          cache.pop
      assert(cache.length <= cache.getMaxSize)

    }

  }

  private def checkId(id: Int): Unit = {
    try {
      require(id == getRuntimeContext.getIndexOfThisSubtask,
        s"FlinkSpoke ID is not equal to the Index of the Flink Subtask")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def nodeFactory: NodeGenerator = man.runtimeClass.newInstance().asInstanceOf[NodeGenerator]

}