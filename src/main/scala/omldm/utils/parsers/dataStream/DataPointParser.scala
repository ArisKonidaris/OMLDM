package omldm.utils.parsers.dataStream

import ControlAPI.DataInstance
import mlAPI.math.{DenseVector, LabeledPoint, Point, UnlabeledPoint, Vector}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Converts a [[DataInstance]] object to a [[Point]] object.
  */
class DataPointParser() extends RichFlatMapFunction[DataInstance, Point] {

  override def flatMap(input: DataInstance, collector: Collector[Point]): Unit = {

    // TODO: Remove this line after the implementation of ML methods that use Discrete Features.
    if (input.getNumericFeatures == null || input.getDiscreteFeatures != null || input.getCategoricalFeatures != null)
      return

    {
      if (input.getOperation.equals("training")) {

        val features: (Vector, Vector, Array[String]) = {
          (
            if (input.getNumericFeatures == null)
              DenseVector()
            else
              DenseVector(input.getNumericFeatures.asInstanceOf[java.util.List[Double]].asScala.toArray),
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
          Some(LabeledPoint(input.getTarget, features._1, features._2, features._3))
        else
          Some(UnlabeledPoint(features._1, features._2, features._3))

      } else None
    } match {
      case Some(point: Point) => collector.collect(point)
      case _ => println("Unknown DataInstance type.")
    }

  }

  override def open(parameters: Configuration): Unit = {}

}
