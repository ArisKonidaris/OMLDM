package omldm.utils.parsers.requestStream

import BipartiteTopologyAPI.sites.NodeId
import ControlAPI.{PreprocessorPOJO, Request}
import omldm.messages.ControlMessage
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class PipelineMap() extends RichFlatMapFunction[Request, ControlMessage] {

  private var nodeMap: MapState[Int, Request] = _

  override def flatMap(request: Request, collector: Collector[ControlMessage]): Unit = {

    implicit val out: Collector[ControlMessage] = collector

    if (request.isValid) {
      if (request.getLearner != null && !ValidLists.learners.contains(request.getLearner.getName))
        return
      if (request.getPreProcessors != null &&
        !(for (pp: PreprocessorPOJO <- request.getPreProcessors.asScala.toList)
          yield ValidLists.preprocessors.contains(pp.getName)
          ).reduce((x,y) => x && y)
      )
        return
      if (!nodeMap.contains(request.getId) && request.getRequest == "Create") {
        nodeMap.put(request.getId, request)
        broadcastControlMessage(request)
        println(s"Pipeline ${request.getId} created.")
      } else if (request.getRequest == "Update" && nodeMap.contains(request.getId)) {
        broadcastControlMessage(request)
      } else if (request.getRequest == "Query" && nodeMap.contains(request.getId)) {
//        val learnerName = nodeMap.get(request.getId).getLearner.getName
//        if (learnerName.equals("HT") || learnerName.equals("K-means"))
//          sendControlMessage(request)
//        else
//          broadcastControlMessage(request)
        sendControlMessage(request)
      } else if (request.getRequest == "Delete" && nodeMap.contains(request.getId)) {
        nodeMap.remove(request.getId)
        broadcastControlMessage(request)
        println(s"Pipeline ${request.getId} deleted.")
      }
    }
  }

  private def sendControlMessage(request: Request)(implicit collector: Collector[ControlMessage]): Unit =
    collector.collect(ControlMessage(request.getId, null, null, new NodeId(null, 0), null, request))

  private def broadcastControlMessage(request: Request)(implicit collector: Collector[ControlMessage]): Unit = {
    for (i <- 0 until getRuntimeContext.getExecutionConfig.getParallelism)
      collector.collect(ControlMessage(request.getId, null, null, new NodeId(null, i), null, request))
  }

  override def open(parameters: Configuration): Unit = {
    nodeMap = getRuntimeContext.getMapState(
      new MapStateDescriptor[Int, Request]("nodeMap",
        createTypeInformation[Int],
        createTypeInformation[Request]))
  }

  object ValidLists {
    val preprocessors: List[String] = List("PolynomialFeatures", "StandardScaler", "MinMaxScaler")
    val learners: List[String] = List("PA", "RegressorPA", "ORR", "SVM", "MultiClassPA", "K-means", "NN", "HT")
  }

}
