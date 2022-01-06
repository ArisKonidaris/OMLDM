package omldm.utils

import ControlAPI.{CountableSerial, QueryResponse}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ResponseConstructor(var testSetSize: Int) extends RichFlatMapFunction[CountableSerial, CountableSerial] {

  private val requestMap: mutable.HashMap[Long, ListBuffer[QueryResponse]] = mutable.HashMap[Long, ListBuffer[QueryResponse]]()

  override def flatMap(in: CountableSerial, collector: Collector[CountableSerial]): Unit = {
    in match {
      case qr: QueryResponse =>
        if (qr.getResponseId == -1)
          collector.collect(qr)
        else {
          if (qr.getLoss == null)
            collector.collect(qr)
          else {
            if (!requestMap.contains(qr.getResponseId)) {
              val newResponseList = ListBuffer[QueryResponse]()
              newResponseList += qr
              requestMap.put(qr.getResponseId, newResponseList)
            } else {
              val newResponseList = requestMap(qr.getResponseId) += qr
              if (newResponseList.length == parallelism) {
                requestMap.remove(qr.getResponseId)
                val finalResponse = newResponseList.head
//                finalResponse.setScore(finalResponse.getScore * (getTestSetSize * 1.0))
                for (resp <- newResponseList.tail) {
                  if (resp.getPreprocessors != null)
                    finalResponse.setPreprocessors(resp.getPreprocessors)
                  if (resp.getLearner != null)
                    finalResponse.setLearner(resp.getLearner)
                  if (resp.getProtocol != null)
                    finalResponse.setProtocol(resp.getProtocol)
                  finalResponse.setDataFitted(finalResponse.getDataFitted + resp.getDataFitted)
                  finalResponse.setLoss(finalResponse.getLoss + resp.getLoss)
                  finalResponse.setCumulativeLoss(finalResponse.getCumulativeLoss + resp.getCumulativeLoss)
//                  finalResponse.setScore(finalResponse.getScore + resp.getScore * (getTestSetSize * 1.0))
                  finalResponse.setScore(finalResponse.getScore + resp.getScore)
                }
                finalResponse.setLoss(finalResponse.getLoss / (parallelism * 1.0))
                finalResponse.setCumulativeLoss(finalResponse.getCumulativeLoss / (parallelism * 1.0))
//                finalResponse.setScore(finalResponse.getScore / (getTestSetSize * parallelism * 1.0))
                finalResponse.setScore(finalResponse.getScore / (parallelism * 1.0))
                collector.collect(finalResponse)
              } else
                requestMap.put(qr.getResponseId, newResponseList)
            }
          }
        }
      case _ =>
    }
  }

  def parallelism: Int = getRuntimeContext.getExecutionConfig.getParallelism

  def getTestSetSize: Int = testSetSize

  def setTestSetSize(testSetSize: Int): Unit = this.testSetSize = testSetSize

}
