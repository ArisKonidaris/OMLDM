package omldm.utils.generators

import BipartiteTopologyAPI.NodeInstance
import ControlAPI.Request
import mlAPI.mlParameterServers.parameterServers.{AsynchronousParameterServer, SynchronousParameterServer}
import mlAPI.mlworkers.MLPredictor
import mlAPI.mlworkers.worker.MLPeriodicWorker

import scala.collection.mutable
import scala.collection.JavaConverters._

case class MLNodeGenerator() extends NodeGenerator {

  override def generateSpokeNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTraining_configuration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[String] match {
            case "Asynchronous" => MLPeriodicWorker().configureWorker(request)
            case "Synchronous" => MLPeriodicWorker().configureWorker(request)
            case "DynamicAveraging" => MLPeriodicWorker().configureWorker(request)
            case "FGMAveraging" => MLPeriodicWorker().configureWorker(request)
            case _ => MLPeriodicWorker().configureWorker(request)
          }
        } catch {
            case _: Throwable => MLPeriodicWorker().configureWorker(request)
        }
      else MLPeriodicWorker().configureWorker(request)
    } catch {
      case e: Exception => throw new RuntimeException("Something went wrong while creating a new ML Flink Spoke.", e)
    }
  }

  override def generateHubNode(request: Request): NodeInstance[_, _] = {
    try {
      val config: mutable.Map[String, AnyRef] = request.getTraining_configuration.asScala
      if (config.contains("protocol"))
        try {
          config.get("protocol").asInstanceOf[String] match {
            case "Asynchronous" => AsynchronousParameterServer().configureParameterServer(request)
            case "Synchronous" => SynchronousParameterServer().configureParameterServer(request)
            case "DynamicAveraging" => AsynchronousParameterServer().configureParameterServer(request)
            case "FGMAveraging" => AsynchronousParameterServer().configureParameterServer(request)
            case _ => AsynchronousParameterServer().configureParameterServer(request)
          }
        } catch {
          case _: Throwable => AsynchronousParameterServer().configureParameterServer(request)
        }
      else AsynchronousParameterServer().configureParameterServer(request)
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
