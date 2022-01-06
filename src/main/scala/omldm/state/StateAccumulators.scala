package omldm.state

import BipartiteTopologyAPI.GenericWrapper
import ControlAPI.Statistics
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

import java.util.stream.Collectors
import java.{lang, util}

class Counter(counter: Long) {
  def this() = this(0)
}

class ParameterAccumulator(val params: lr_params) {
  def this() = this(VectorBias(BreezeDenseVector.zeros[Double](1), 0.0))
}

class DataQueueAccumulator(val dataSet: mutable.Queue[Point]) {
  def this() = this(mutable.Queue[Point]())
}

class DataListAccumulator(val dataSet: ListBuffer[Point]) {
  def this() = this(ListBuffer[Point]())
}

class SpokeMessageAccumulator(val dataSet: DataSet[SpokeMessage]) {
  def this() = this(new DataSet[SpokeMessage](20000))
}

class NodeAccumulator(val node: GenericWrapper) {

  def this() = this(null)

  def getNodeWrapper: GenericWrapper = node

}

class StatisticsAccumulator(val mlpHubIds: mutable.HashMap[Int, ListBuffer[String]],
                            val stats: mutable.HashMap[String, Statistics]) {
  def this() = this(new mutable.HashMap[Int, ListBuffer[String]](), new mutable.HashMap[String, Statistics]())
}

class StatisticsAggregateFunction()
  extends AggregateFunction[
    (String, Statistics),
    StatisticsAccumulator,
    mutable.HashMap[Int, Statistics]] {

  override def createAccumulator(): StatisticsAccumulator = new StatisticsAccumulator()

  override def add(in: (String, Statistics), acc: StatisticsAccumulator)
  : StatisticsAccumulator = {
    if (!acc.mlpHubIds.contains(in._2.getPipeline)) {
      assert(!acc.stats.contains(in._1))
      acc.mlpHubIds.put(in._2.getPipeline, new ListBuffer[String]())
      acc.mlpHubIds(in._2.getPipeline) += in._1
      acc.stats.put(in._1, in._2)
    } else {
      if (!acc.stats.contains(in._1)) {
        assert(!acc.mlpHubIds(in._2.getPipeline).contains(in._1))
        acc.mlpHubIds(in._2.getPipeline) += in._1
        acc.stats.put(in._1, in._2)
      } else {
        if (in._1.split("_")(2) == "0" && acc.stats(in._1).getLearningCurve != null) {
          if (in._2.getLearningCurve != null) {
            val y: util.ArrayList[lang.Double] = new util.ArrayList(acc.stats(in._1).getLearningCurve)
            y.addAll(in._2.getLearningCurve)
            val x: util.ArrayList[lang.Long] = new util.ArrayList(acc.stats(in._1).getLCX)
            x.addAll(in._2.getLCX)
            in._2.setLearningCurve(y)
            in._2.setLCX(x)
          } else {
            in._2.setLearningCurve(acc.stats(in._1).getLearningCurve)
            in._2.setLCX(acc.stats(in._1).getLCX)
          }
        }
        acc.stats(in._1) = in._2
      }
    }
    acc
  }

  override def getResult(acc: StatisticsAccumulator): mutable.HashMap[Int, Statistics] = {
    val result: mutable.HashMap[Int, Statistics] = new mutable.HashMap[Int, Statistics]()
    for (mhi <- acc.mlpHubIds.iterator) {
      result.put(mhi._1, new Statistics(mhi._1))
      for (hi: String <- mhi._2) {
        if (result(mhi._1).getProtocol.equals(""))
          result(mhi._1).setProtocol(acc.stats(hi).getProtocol)
        result(mhi._1).updateStats(acc.stats(hi))
      }
      result(mhi._1).setBlocks(result(mhi._1).getBlocks / mhi._2.length)
      result(mhi._1).setModels(result(mhi._1).getModels / mhi._2.length)
      result(mhi._1).setFitted(result(mhi._1).getFitted / mhi._2.length)
    }
    result
  }

  override def merge(acc: StatisticsAccumulator, acc1: StatisticsAccumulator): StatisticsAccumulator = {
    for (entry: (Int, ListBuffer[String]) <- acc1.mlpHubIds)
      if (!acc.mlpHubIds.contains(entry._1))
        acc.mlpHubIds.put(entry._1, entry._2)
      else
        for (hi: String <-entry._2)
          if (!acc.mlpHubIds(entry._1).contains(hi))
            acc.mlpHubIds(entry._1) += hi
    for (entry: (String, Statistics)  <- acc1.stats)
      if (!acc.stats.contains(entry._1))
        acc.stats.put(entry._1, entry._2)
      else
        acc.stats(entry._1).updateStats(entry._2)
    acc
  }

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

