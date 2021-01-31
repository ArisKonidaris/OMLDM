package omldm.utils.parsers.dataStream

import ControlAPI.DataInstance
import mlAPI.math.{DenseVector, ForecastingPoint, LabeledPoint, LearningPoint, TrainingPoint, UnlabeledPoint, UsablePoint, Vector}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Converts a [[DataInstance]] object to a [[Point]] object.
  */
class DataPointParser() extends RichFlatMapFunction[DataInstance, UsablePoint] {

  override def flatMap(input: DataInstance, collector: Collector[UsablePoint]): Unit = {
    if (input.getNumericalFeatures == null && input.getDiscreteFeatures == null && input.getCategoricalFeatures == null)
      return
    {
      val point: LearningPoint = {
        val features: (Vector, Vector, Array[String]) = {
          (
            if (input.getNumericalFeatures == null)
              DenseVector()
            else
              DenseVector(input.getNumericalFeatures.asInstanceOf[java.util.List[Double]].asScala.toArray),
            if (input.getDiscreteFeatures == null)
              DenseVector()
            else
              DenseVector(input.getDiscreteFeatures.asInstanceOf[java.util.List[Int]].asScala.toArray.map(x => x.toDouble)),
            if (input.getCategoricalFeatures == null)
              Array[String]()
            else
              input.getCategoricalFeatures.asScala.toArray
          )
        }
        if (input.getTarget != null)
          LabeledPoint(input.getTarget, features._1, features._2, features._3, input.toString)
        else
          UnlabeledPoint(features._1, features._2, features._3, input.toString)
      }
      if (input.getOperation.equals("training")) {
        point.dataInstance = null
        Some(TrainingPoint(point))
      } else if (input.getOperation.equals("forecasting"))
        Some(ForecastingPoint(point.asUnlabeledPoint))
      else
        None
    } match {
      case Some(point: TrainingPoint) => collector.collect(point)
      case Some(point: ForecastingPoint) => collector.collect(point)
      case None => println("Unknown Point type.")
      case _ => println("Unknown Point type.")
    }
  }

  override def open(parameters: Configuration): Unit = {}

}
