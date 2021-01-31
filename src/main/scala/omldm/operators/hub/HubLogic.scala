package omldm.operators.hub

import BipartiteTopologyAPI.GenericWrapper
import org.apache.flink.api.common.state.AggregatingState
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.io.Serializable

/** Basic abstract operator of a coordinator in Flink.
  *
  * @tparam InMsg  The worker message type accepted by the coordinator.
  * @tparam OutMsg The output message type emitted by the coordinator.
  */
abstract class HubLogic[InMsg <: Serializable, OutMsg <: Serializable]
  extends KeyedProcessFunction[String, InMsg, OutMsg]
    with Hub {

  protected var jobParallelism: Int = 0

  protected var state: AggregatingState[
    (InMsg, KeyedProcessFunction[String, InMsg, OutMsg]#Context, Collector[OutMsg]),
    GenericWrapper
  ]

  protected var cache: AggregatingState[InMsg, Option[InMsg]]

  def parallelism: Int = {
    jobParallelism = getRuntimeContext.getExecutionConfig.getParallelism
    jobParallelism
  }

  def checkParallelism: Boolean = jobParallelism == getRuntimeContext.getExecutionConfig.getParallelism

}
