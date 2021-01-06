package omldm.utils.parsers

import ControlAPI.DataInstance
import org.apache.flink.api.common.functions.RichFlatMapFunction
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.util.Collector

case class DataInstanceParser() extends RichFlatMapFunction[String, DataInstance] {

  private val mapper: ObjectMapper = new ObjectMapper()

  override def flatMap(record: String, collector: Collector[DataInstance]): Unit = {
    try {
      if (!record.equals("EOS")) {
        val dataInstance = mapper.readValue(record, classOf[DataInstance])
        if (dataInstance.isValid)
          collector.collect(dataInstance)
      }
    } catch {
      case _: Throwable =>
    }
  }

}
