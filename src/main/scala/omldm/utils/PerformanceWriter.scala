package omldm.utils

import ControlAPI.JobStatistics
import org.apache.flink.api.common.functions.MapFunction

class PerformanceWriter() extends MapFunction[JobStatistics, String] {
  override def map(performance: JobStatistics): String = performance.toString
}
