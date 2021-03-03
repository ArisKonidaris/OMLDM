package omldm.operators.spoke

import BipartiteTopologyAPI.BufferingWrapper
import BipartiteTopologyAPI.interfaces.Node
import BipartiteTopologyAPI.operations.RemoteCallIdentifier
import BipartiteTopologyAPI.sites.{NodeId, NodeType}
import ControlAPI.Request
import mlAPI.dataBuffers.DataSet
import mlAPI.math.{ForecastingPoint, TrainingPoint, UsablePoint}
import mlAPI.protocols.IntWrapper
import mlAPI.utils.Parsing
import omldm.messages.{ControlMessage, SpokeMessage}
import omldm.network.FlinkNetwork
import omldm.utils.generators.NodeGenerator
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import java.io.Serializable
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.Manifest
import scala.util.Random

/** A CoFlatMap Flink Function modelling a worker request in a star distributed topology. */
class FlinkSpoke[G <: NodeGenerator](var testSetSize: Int,
                                     var test: Boolean,
                                     var maxMsgParams: Int,
                                     val spokeParallelism: IntWrapper)(implicit man: Manifest[G])
  extends SpokeLogic[UsablePoint, ControlMessage, SpokeMessage] {

  /** A counter used to sample data points for testing the score of the model. */
  private var count: Int = 0

  /** A counter used for testing the performance of the training job. */
  private var testingCount: Int = 0

  /** The test set buffer. */
  private var testSet: DataSet[UsablePoint] = new DataSet[UsablePoint](testSetSize)

  /** Flink List State variables for storing the Flink Spoke state. */
  private var savedTestSet: ListState[DataSet[UsablePoint]] = _
  private var nodes: ListState[scala.collection.mutable.Map[Int, BufferingWrapper[UsablePoint]]] = _
  private var savedRecordBuffer: ListState[DataSet[UsablePoint]] = _
  private var savedRequestBuffer: ListState[DataSet[ControlMessage]] = _

  private var collector: Collector[SpokeMessage] = _
  private var context: CoProcessFunction[UsablePoint, ControlMessage, SpokeMessage]#Context = _

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
  override def processElement1(data: UsablePoint,
                               ctx: CoProcessFunction[UsablePoint, ControlMessage, SpokeMessage]#Context,
                               out: Collector[SpokeMessage]): Unit = {
    collector = out
    context = ctx
    checkParallelism()
    if (getJobParallelism == spokeParallelism.getInt && requestBuffer.nonEmpty)
      while (requestBuffer.nonEmpty)
        createWrapper(requestBuffer.pop.get)
    if (state.nonEmpty) {
      if (recordBuffer.nonEmpty) {
        recordBuffer.append(data)
        while (recordBuffer.nonEmpty)
          handleData(recordBuffer.pop.get)
      } else
        handleData(data)
    } else
      recordBuffer.append(data)

    // For calculating the performance of the training procedure.
    if (test && getJobParallelism == spokeParallelism.getInt) {
      testingCount += 1
      if (testingCount == 100) {
        testingCount = 0
        out.collect(SpokeMessage(-1, null, null, null, null, null))
      }
    }
  }

  private def handleData(data: UsablePoint): Unit = {
//    if (getRuntimeContext.getIndexOfThisSubtask == 0) {
      data match {
        case trainingPoint: TrainingPoint =>
          if (count >= 8) {
            testSet.append(trainingPoint) match {
              case Some(point: Serializable) => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
              case None =>
            }
          } else
            for ((_, node: Node) <- state) node.receiveTuple(Array[Any](trainingPoint))
          count += 1
          if (count == 10)
            count = 0
        case forecastingPoint: ForecastingPoint => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](forecastingPoint))
      }
//    } else {
//      data match {
//        case trainingPoint: TrainingPoint =>
//          if (testSet.nonEmpty) {
//            testSet.append(trainingPoint) match {
//              case Some(point: Serializable) => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
//              case None =>
//            }
//            while (testSet.nonEmpty) {
//              val point = testSet.pop.get
//              for ((_, node: Node) <- state) node.receiveTuple(Array[Any](point))
//            }
//          } else
//            for ((_, node: Node) <- state) node.receiveTuple(Array[Any](data))
//        case forecastingPoint: ForecastingPoint => for ((_, node: Node) <- state) node.receiveTuple(Array[Any](forecastingPoint))
//      }
//    }
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
                      ctx: CoProcessFunction[UsablePoint, ControlMessage, SpokeMessage]#Context,
                      out: Collector[SpokeMessage]): Unit = {
    collector = out
    context = ctx
    checkParallelism()
    message match {
      case ControlMessage(network, operation, source, destination, data, request) =>
        checkId(destination.getNodeId)
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
                    node.receiveQuery(-1, (node.getMeanBufferSize / (getJobParallelism * 1.0), testSet.dataBuffer.toArray))
                else
                  println(s"Empty request in worker ${getRuntimeContext.getIndexOfThisSubtask}.")
              case req: Request =>
                req.getRequest match {

                  case "Create" =>
                    if (getJobParallelism < spokeParallelism.getInt)
                      requestBuffer.append(message)
                    else {
                      assert(getJobParallelism == spokeParallelism.getInt)
                      if (requestBuffer.isEmpty)
                        createWrapper(message)
                      else {
                        requestBuffer.append(message)
                        while (requestBuffer.nonEmpty)
                          createWrapper(requestBuffer.pop.get)
                      }
                    }

                  case "Update" =>

                  case "Query" =>
                    if (req.getRequestId != null)
                      if (state.contains(network))
                        state(network).receiveQuery(req.getRequestId, (0, testSet.dataBuffer.toArray))

                  case "Delete" =>
                    if (state.contains(network))
                      state.remove(network)

                  case _: String =>
                    println(s"Invalid req type in worker ${getRuntimeContext.getIndexOfThisSubtask}.")
                }
            }
        }
    }
  }

  def createWrapper(msg: ControlMessage): Unit = {
    if (!state.contains(msg.getNetworkId)) {
      val parallelJob: Boolean = getJobParallelism > 1
      var singleLearner: Boolean = false
      val hubParallelism: Int = {
        if (parallelJob)
          try {
            Parsing.IntegerParsing(msg.getRequest.getTrainingConfiguration.asScala,"HubParallelism", 1)
          } catch {
            case _: Throwable => 1
          }
        else
          1
      }
      val flinkNetwork = FlinkNetwork[UsablePoint, ControlMessage, SpokeMessage](
        NodeType.SPOKE,
        msg.getNetworkId,
        spokeParallelism,
        hubParallelism)
      flinkNetwork.setCollector(collector)
      flinkNetwork.setContext(context)
      state += (
        msg.getNetworkId -> new BufferingWrapper(
          new NodeId(NodeType.SPOKE, getRuntimeContext.getIndexOfThisSubtask),
          {
            if (parallelJob) {
              msg.getRequest.getLearner.name match {
                case "HT" =>
                  msg.getRequest.getTrainingConfiguration.replace("protocol", "SingleLearner".asInstanceOf[AnyRef])
                  singleLearner = true
                case "K-means" =>
                  msg.getRequest.getTrainingConfiguration.replace("protocol", "SingleLearner".asInstanceOf[AnyRef])
                  singleLearner = true
                case _ =>
              }
              nodeFactory.generateSpokeNode(msg.getRequest)
            } else {
              msg.getRequest.getTrainingConfiguration.replace("protocol", "CentralizedTraining".asInstanceOf[AnyRef])
              nodeFactory.generateSpokeNode(msg.getRequest)
            }
          },
          flinkNetwork)
        )
      if (getRuntimeContext.getIndexOfThisSubtask == 0 && !singleLearner)
        for (i <- 0 until hubParallelism)
          collector.collect(SpokeMessage(msg.getNetworkId, null, null, new NodeId(NodeType.HUB, i), null, msg.getRequest))
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
    savedRecordBuffer.clear()
    savedRecordBuffer add recordBuffer
    savedRequestBuffer.clear()
    savedRequestBuffer add requestBuffer

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
      new ListStateDescriptor[scala.collection.mutable.Map[Int, BufferingWrapper[UsablePoint]]]("node",
        TypeInformation.of(new TypeHint[scala.collection.mutable.Map[Int, BufferingWrapper[UsablePoint]]]() {}))
    )

    savedTestSet = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[UsablePoint]]("savedTestSet",
        TypeInformation.of(new TypeHint[DataSet[UsablePoint]]() {}))
    )

    savedRecordBuffer = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[UsablePoint]]("savedRecordBuffer",
        TypeInformation.of(new TypeHint[DataSet[UsablePoint]]() {}))
    )

    savedRequestBuffer = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[DataSet[ControlMessage]]("savedRequestBuffer",
        TypeInformation.of(new TypeHint[DataSet[ControlMessage]]() {}))
    )

    // ============================================ Restart strategy ===================================================

    if (context.isRestored) {

      // ======================================== Restoring the Spokes =================================================

      state.clear()

      var new_state = scala.collection.mutable.Map[Int, BufferingWrapper[UsablePoint]]()
      val it_pip = nodes.get.iterator
      val remoteStates = ListBuffer[scala.collection.mutable.Map[Int, BufferingWrapper[UsablePoint]]]()
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

      recordBuffer.clear()
      recordBuffer = mergingDataBuffers(savedRecordBuffer)
      if (state.nonEmpty)
        while (recordBuffer.nonEmpty)
          handleData(recordBuffer.pop.get)
      else
        while (recordBuffer.length > recordBuffer.getMaxSize)
          recordBuffer.pop
      assert(recordBuffer.length <= recordBuffer.getMaxSize)

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

  private def checkParallelism(): Unit = {
    if (getJobParallelism > spokeParallelism.getInt)
      spokeParallelism.setInt(getJobParallelism)
  }

  private def nodeFactory: NodeGenerator =
    man.runtimeClass.newInstance().asInstanceOf[NodeGenerator].setMaxMsgParams(maxMsgParams)

  def getTestSetSize: Int = testSetSize

  def setTestSetSize(testSetSize: Int): Unit = this.testSetSize = testSetSize

}