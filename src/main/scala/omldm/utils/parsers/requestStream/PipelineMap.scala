package omldm.utils.parsers.requestStream

import BipartiteTopologyAPI.sites.NodeId
import ControlAPI.{Preprocessor, Request}
import omldm.messages.ControlMessage
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class PipelineMap() extends RichFlatMapFunction[Request, ControlMessage] {

  private var node_map: MapState[Int, Request] = _

  override def flatMap(request: Request, collector: Collector[ControlMessage]): Unit = {

    implicit val out: Collector[ControlMessage] = collector

    if (request.isValid) {
      if (request.getLearner != null && !ValidLists.learners.contains(request.getLearner.getName)) return
      if (request.getPreprocessors != null &&
        !(for (pp: Preprocessor <- request.getPreprocessors.asScala.toList)
          yield ValidLists.preprocessors.contains(pp.getName)
          ).reduce((x,y) => x && y)
      ) return
      if (!node_map.contains(request.getId) && request.getRequest == "Create") {
        node_map.put(request.getId, request)
        broadcastControlMessage(request)
        println(s"Pipeline ${request.getId} created.")
      } else if (request.getRequest == "Update" && node_map.contains(request.getId)) {
        broadcastControlMessage(request)
      } else if (request.getRequest == "Query" && node_map.contains(request.getId)) {
        sendControlMessage(request)
      } else if (request.getRequest == "Delete" && node_map.contains(request.getId)) {
        node_map.remove(request.getId)
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
    node_map = getRuntimeContext.getMapState(
      new MapStateDescriptor[Int, Request]("node_map",
        createTypeInformation[Int],
        createTypeInformation[Request]))
  }

  object ValidLists {
    val preprocessors: List[String] = List("PolynomialFeatures", "StandardScaler")
    val learners: List[String] = List("PA", "regressorPA", "ORR", "SVM", "MultiClassPA", "KMeans")
  }

}
