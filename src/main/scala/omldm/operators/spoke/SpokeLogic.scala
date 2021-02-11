package omldm.operators.spoke

import BipartiteTopologyAPI.BufferingWrapper
import BipartiteTopologyAPI.interfaces.Mergeable
import mlAPI.dataBuffers.DataSet
import org.apache.flink.api.common.state.ListState
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.CoProcessFunction

import java.io.Serializable
import scala.collection.mutable.ListBuffer

/** An abstract operator of a stateful Flink remote spoke. A spoke like this can possess multiple [[BufferingWrapper]]
 * instances that all process the incoming data records.
 *
 * @tparam InMsg   The message message type accepted by the spoke
 * @tparam CtrlMsg The control message send by the coordinator to the remote sites
 * @tparam OutMsg  The output message type emitted by the spoke
 */
abstract class SpokeLogic[InMsg <: Serializable, CtrlMsg <: Serializable, OutMsg <: Serializable]
  extends CoProcessFunction[InMsg, CtrlMsg, OutMsg]
    with CheckpointedFunction
    with Spoke {

  protected var jobParallelism: Int = 0

  /** A state variable holding all the Buffering wrapper objects of the current spoke. */
  protected val state: scala.collection.mutable.Map[Int, BufferingWrapper[InMsg]] =
    scala.collection.mutable.Map[Int, BufferingWrapper[InMsg]]()

  /** A buffer for storing arriving records. */
  protected var recordBuffer: DataSet[InMsg] = new DataSet[InMsg](100000)

  /** A buffer for storing arriving requests. */
  protected var requestBuffer: DataSet[CtrlMsg] = new DataSet[CtrlMsg](10000)

  def mergingDataBuffers(saved_buffers: ListState[DataSet[InMsg]]): DataSet[InMsg] = {
    var new_buffer = new DataSet[InMsg]()

    val buffers_iterator = saved_buffers.get.iterator
    val buffers: ListBuffer[Mergeable] = ListBuffer[Mergeable]()
    while (buffers_iterator.hasNext) {
      val next = buffers_iterator.next
      if (next.nonEmpty)
        if (new_buffer.isEmpty) new_buffer = next else buffers.append(next)
    }
    new_buffer.merge(buffers.toArray)

    new_buffer
  }

  def getJobParallelism: Int = {
    jobParallelism = getRuntimeContext.getExecutionConfig.getParallelism
    jobParallelism
  }

  def checkJobParallelism: Boolean = jobParallelism == getRuntimeContext.getExecutionConfig.getParallelism

}
