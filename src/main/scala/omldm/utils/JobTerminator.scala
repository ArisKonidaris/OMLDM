package omldm.utils

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class JobTerminator(jobName: String) extends RichFlatMapFunction[String, String] {
  override def flatMap(in: String, collector: Collector[String]): Unit = {
    throw new Exception("End of job " + jobName + ".")
  }
}
