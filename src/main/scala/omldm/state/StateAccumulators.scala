package omldm.state

import BipartiteTopologyAPI.GenericWrapper
import breeze.linalg.{DenseVector => BreezeDenseVector}
import mlAPI.dataBuffers.DataSet
import mlAPI.math.Point
import mlAPI.parameters.{VectorBias, LearningParameters => lr_params}
import omldm.messages.{HubMessage, SpokeMessage}
import omldm.network.FlinkNetwork
import org.apache.flink.api.common.functions.AggregateFunction

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class Counter(counter: Long) {
  def this() = this(0)
}

class ParameterAccumulator(params: lr_params) {
  def this() = this(VectorBias(BreezeDenseVector.zeros[Double](1), 0.0))
}

class DataQueueAccumulator(dataSet: mutable.Queue[Point]) {
  def this() = this(mutable.Queue[Point]())
}

class DataListAccumulator(dataSet: ListBuffer[Point]) {
  def this() = this(ListBuffer[Point]())
}

class SpokeMessageAccumulator(val dataSet: DataSet[SpokeMessage]) {
  def this() = this(new DataSet[SpokeMessage](20000))
}

class NodeAccumulator(node: GenericWrapper) {

  def this() = this(null)

  def getNodeWrapper: GenericWrapper = node
}

class DataAggregateFunction(max_size: Int)
  extends AggregateFunction[SpokeMessage, SpokeMessageAccumulator, Option[SpokeMessage]] {

  def this() = this(20000)

  override def createAccumulator(): SpokeMessageAccumulator = new SpokeMessageAccumulator(new DataSet[SpokeMessage](max_size))

  override def add(in: SpokeMessage, acc: SpokeMessageAccumulator): SpokeMessageAccumulator = {
    acc.dataSet.append(in)
    acc
  }

  override def getResult(acc: SpokeMessageAccumulator): Option[SpokeMessage] = acc.dataSet.pop

  override def merge(acc1: SpokeMessageAccumulator, acc2: SpokeMessageAccumulator): SpokeMessageAccumulator = {
    acc1.dataSet.merge(Array(acc2.dataSet))
    acc1
  }
}

class NodeAggregateFunction()
  extends AggregateFunction[
    (SpokeMessage, KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context, Collector[HubMessage]),
    NodeAccumulator, GenericWrapper
  ] {

  override def createAccumulator(): NodeAccumulator = new NodeAccumulator()

  override def add(message: (
    SpokeMessage,
      KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context,
      Collector[HubMessage]
    ), acc: NodeAccumulator): NodeAccumulator = {

    message._1.getData match {
      case wrapper: GenericWrapper =>
        val newNodeAccumulator = new NodeAccumulator(wrapper)
        setCollectors(newNodeAccumulator, message._2, message._3)
        newNodeAccumulator
      case _ =>
        setCollectors(acc, message._2, message._3)
        acc.getNodeWrapper.receiveMsg(message._1.source, message._1.getOperation, message._1.getData)
        acc
    }

  }

  override def getResult(acc: NodeAccumulator): GenericWrapper = acc.getNodeWrapper

  override def merge(acc: NodeAccumulator, acc1: NodeAccumulator): NodeAccumulator = {
    acc.getNodeWrapper.merge(Array(acc1.getNodeWrapper))
    acc
  }

  private def setCollectors(a: NodeAccumulator,
                            ctx: KeyedProcessFunction[String, SpokeMessage, HubMessage]#Context,
                            out: Collector[HubMessage]): Unit = {
    val flinkNetwork = a.getNodeWrapper.getNetwork
      .asInstanceOf[FlinkNetwork[SpokeMessage, HubMessage, HubMessage]]
    flinkNetwork.setKeyedContext(ctx)
    flinkNetwork.setCollector(out)
  }


}

