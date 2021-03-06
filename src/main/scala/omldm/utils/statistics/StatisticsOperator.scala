package omldm.utils.statistics

import ControlAPI.{JobStatistics, Statistics}
import omldm.state.{StatisticsAccumulator, StatisticsAggregateFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import omldm.Job.terminationStats
import omldm.messages.HubMessage

import scala.collection.mutable

/**
 * A Flink KeyedProcessFunction operator that gathers the communication and performance statistics
 * of the Machine Learning Pipelines that are trained on the job.
 *
 * @param jobName The job name of the Flink Job.
 */
class StatisticsOperator(val jobName: String,
                         var testSetSize: Int,
                         var timeout: Long)
  extends KeyedProcessFunction[Int, (String, Statistics), JobStatistics]{

  /** The starting time of the distributed training. */
  private var start: ValueState[Long] = _

  /** The ending time of the distributed training. */
  private var end: ValueState[Long] = _

  /** The timestamp of the latest message. */
  private var timestampState: ValueState[Long] = _

  /** The statistics of the ML pipelines running on this training job. */
  protected var statsAccumulator: AggregatingState[(String, Statistics), mutable.HashMap[Int, Statistics]] = _

  /**
   * A counter for determining if all the pipeline statistics have been gathered
   * in order to terminate the training job.
   */
  protected var counter: ValueState[Int] = _

  /** The final statistics of the training job. */
  protected var finalJobStats: ValueState[mutable.HashMap[Int, Statistics]] = _

  override def open(parameters: Configuration): Unit = {
    start = getRuntimeContext.getState(new ValueStateDescriptor[Long]("startTimestamp", classOf[Long], -1L))
    end = getRuntimeContext.getState(new ValueStateDescriptor[Long]("endTimestamp", classOf[Long]))
    timestampState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timestampState", classOf[Long]))
    statsAccumulator = getRuntimeContext.getAggregatingState[
      (String, Statistics),
      StatisticsAccumulator,
      mutable.HashMap[Int, Statistics]](
      new AggregatingStateDescriptor(
        "jobStatistics",
        new StatisticsAggregateFunction(),
        createTypeInformation[StatisticsAccumulator]))
    counter = getRuntimeContext.getState(new ValueStateDescriptor[Int]("completeStats", classOf[Int]))
    finalJobStats = getRuntimeContext.getState(
      new ValueStateDescriptor[mutable.HashMap[Int, Statistics]](
        "finalJobStats",
        classOf[mutable.HashMap[Int, Statistics]],
        new mutable.HashMap[Int, Statistics]()
      )
    )
  }

  override def processElement(msgStats: (String, Statistics),
                              ctx: KeyedProcessFunction[Int, (String, Statistics), JobStatistics]#Context,
                              collector: Collector[JobStatistics])
  : Unit = {

    if (!msgStats._1.equals("Terminate")) {

      // Update the timers.
      if (start.value() < 0L && !msgStats._1.equals(""))
        start.update(ctx.timestamp())
      else
        end.update(ctx.timestamp())

      // Update the statistics.
      if (!msgStats._1.equals(""))
        statsAccumulator add msgStats

      // Set the state's timestamp to the record's assigned timestamp.
      val tempTime = ctx.timestamp()
      timestampState.update(tempTime)

      // Schedule the next timer timeout ms from the current record time.
      ctx.timerService.registerEventTimeTimer(tempTime + timeout)

    } else {

      if (finalJobStats.value().isEmpty)
        finalJobStats update statsAccumulator.get()
      val s = finalJobStats.value()
      println(s)
      assert(s.contains(msgStats._2.getPipeline))
      if (s(msgStats._2.getPipeline).getProtocol != "SingleLearner") {
        s(msgStats._2.getPipeline).updateMeanBufferSize(msgStats._2.getMeanBufferSize)
        s(msgStats._2.getPipeline).updateFitted(msgStats._2.getFitted)
        s(msgStats._2.getPipeline).updateScore(msgStats._2.getScore * (getTestSetSize * 1.0))
      } else
        s(msgStats._2.getPipeline).updateScore(msgStats._2.getScore)
      finalJobStats update s
      var cs = counter.value()
      cs += 1
      if (cs == parallelism * finalJobStats.value().size)
        collector.collect(
          new JobStatistics(
            jobName,
            parallelism,
            end.value() - start.value(),
            finalJobStats.value().toArray.sortBy(_._1).map(x => {
              if (x._2.getProtocol.equals("SingleLearner"))
                x._2.setScore(x._2.getScore / parallelism)
              else {
                println("---> " + x._2.getScore)
                println("---> " + parallelism * getTestSetSize * 1.0)
                println("---> " + x._2.getScore / (parallelism * getTestSetSize * 1.0))
                x._2.setScore(x._2.getScore / (parallelism * getTestSetSize * 1.0))
              }
              x._2
            })
          )
        )
      else
        counter.update(cs)

    }

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Int, (String, Statistics), JobStatistics]#OnTimerContext,
                       out: Collector[JobStatistics])
  : Unit = {
    // Check if this is an outdated timer or the latest timer.
    if (timestamp == timestampState.value + timeout)
      ctx.output(terminationStats, new HubMessage())
  }

  def parallelism: Int = getRuntimeContext.getExecutionConfig.getParallelism

  def getTestSetSize: Int = testSetSize

  def setTestSetSize(testSetSize: Int): Unit = this.testSetSize = testSetSize

}
