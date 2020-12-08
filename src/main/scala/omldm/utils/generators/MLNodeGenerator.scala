package omldm.utils.generators

import BipartiteTopologyAPI.NodeInstance
import ControlAPI.Request
import mlAPI.mlParameterServers.{AsynchronousParameterServer, FGMParameterServer, SynchronousParameterServer}
import mlAPI.mlworkers.MLPredictor
import mlAPI.mlworkers.worker.{AsynchronousWorker, FGMWorker, SynchronousWorker}

import scala.collection.mutable
import scala.collection.JavaConverters._

case class MLNodeGenerator() extends NodeGenerator {

  override def generateSpokeNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTraining_configuration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[Option[String]] match {
            case Some("Asynchronous") => AsynchronousWorker().configureWorker(request)
            case Some("Synchronous") => SynchronousWorker().configureWorker(request)
            case Some("DynamicAveraging") => AsynchronousWorker().configureWorker(request)
            case Some("FGM") => FGMWorker().configureWorker(request)
            case Some(_) => AsynchronousWorker().configureWorker(request)
          }
        } catch {
            case e: Throwable => AsynchronousWorker().configureWorker(request)
        }
      else
        AsynchronousWorker().configureWorker(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating a new ML Flink Spoke.", e)
    }
  }

  override def generateHubNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTraining_configuration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[Option[String]] match {
            case Some("Asynchronous") => AsynchronousParameterServer().configureParameterServer(request)
            case Some("Synchronous") => SynchronousParameterServer().configureParameterServer(request)
            case Some("DynamicAveraging") => AsynchronousParameterServer().configureParameterServer(request)
            case Some("FGM") => FGMParameterServer().configureParameterServer(request)
            case Some(_) => AsynchronousParameterServer().configureParameterServer(request)
          }
        } catch {
          case _: Throwable => AsynchronousParameterServer().configureParameterServer(request)
        }
      else
        AsynchronousParameterServer().configureParameterServer(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating a new ML Flink Hub.", e)
    }
  }

  override def generatePredictorNode(request: Request): NodeInstance[_, _] = {
    try {
      new MLPredictor().configureWorker(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating an ML Predictor Node.", e)
    }
  }
}
