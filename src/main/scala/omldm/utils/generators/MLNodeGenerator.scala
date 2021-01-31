package omldm.utils.generators

import BipartiteTopologyAPI.NodeInstance
import ControlAPI.Request
import mlAPI.mlParameterServers.proto.{AsynchronousParameterServer, EASGDParameterServer, FGMParameterServer, GMParameterServer, SSPParameterServer, SimplePS, SynchronousParameterServer}
import mlAPI.mlworkers.worker.proto.{AsynchronousWorker, EASGDWorker, FGMWorker, GMWorker, SSPWorker, SingleWorker, SynchronousWorker}

import scala.collection.mutable
import scala.collection.JavaConverters._

case class MLNodeGenerator() extends NodeGenerator {

  var maxMsgParams: Int = 10000

  override def setMaxMsgParams(maxMsgParams: Int): MLNodeGenerator = {
    this.maxMsgParams = maxMsgParams
    this
  }

  override def generateSpokeNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTrainingConfiguration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[Option[String]] match {
            case Some("CentralizedTraining") => SingleWorker(maxMsgParams).configureWorker(request)
            case Some("Asynchronous") => AsynchronousWorker(maxMsgParams).configureWorker(request)
            case Some("Synchronous") => SynchronousWorker(maxMsgParams).configureWorker(request)
            case Some("SSP") => SSPWorker(maxMsgParams = maxMsgParams).configureWorker(request)
            case Some("EASGD") => EASGDWorker(maxMsgParams = maxMsgParams).configureWorker(request)
            case Some("GM") => GMWorker(maxMsgParams).configureWorker(request)
            case Some("FGM") => FGMWorker(maxMsgParams = maxMsgParams).configureWorker(request)
            case Some(_) => AsynchronousWorker(maxMsgParams).configureWorker(request)
            case None => AsynchronousWorker(maxMsgParams).configureWorker(request)
          }
        } catch {
            case e: Throwable =>
              e.printStackTrace()
              AsynchronousWorker(maxMsgParams).configureWorker(request)
        }
      else
        AsynchronousWorker(maxMsgParams).configureWorker(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating a new ML Flink Spoke.", e)
    }
  }

  override def generateHubNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTrainingConfiguration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[Option[String]] match {
            case Some("CentralizedTraining") => SimplePS().configureParameterServer(request)
            case Some("Asynchronous") => AsynchronousParameterServer().configureParameterServer(request)
            case Some("Synchronous") => SynchronousParameterServer().configureParameterServer(request)
            case Some("SSP") => SSPParameterServer().configureParameterServer(request)
            case Some("EASGD") => EASGDParameterServer().configureParameterServer(request)
            case Some("GM") => GMParameterServer().configureParameterServer(request)
            case Some("FGM") => FGMParameterServer().configureParameterServer(request)
            case Some(_) => AsynchronousParameterServer().configureParameterServer(request)
            case None => AsynchronousParameterServer().configureParameterServer(request)
          }
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            AsynchronousParameterServer().configureParameterServer(request)
        }
      else
        AsynchronousParameterServer().configureParameterServer(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating a new ML Flink Hub.", e)
    }
  }

}
