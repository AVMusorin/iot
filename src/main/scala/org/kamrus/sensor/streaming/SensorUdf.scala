package org.kamrus.sensor.streaming

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import SensorProcessingUtils._
import org.apache.spark.internal.Logging


object SensorUdf extends Logging {
  val resSchema = StructType(List(
    StructField("value", ArrayType(IntegerType), true)
  ))
  val convertUdf: UserDefinedFunction = udf(covertUdfBody(_: String))
  val smoothUdf: UserDefinedFunction = udf(smoothUdfBody(_: Array[Int]))
  val binaryToStringUdf: UserDefinedFunction = udf(binaryToString(_: Array[Byte]))

  val calculateRowUdf: UserDefinedFunction = udf(calculateRow(_: Array[Byte]))

  def covertUdfBody(values: String, sep: String = " "): Array[Int] = {
    values.split(sep).map(_.toInt)
  }

  def smoothUdfBody(values: Array[Int], alpha: Double = 0.6): Array[Double] = {
    val smoothedArray = Array[Double](values.length)
    smoothedArray(0) = values(0).toDouble
    for (i <- 1 to values.length) {
      smoothedArray(i) = alpha * values(i) + (1 - alpha) * values(i - 1)
    }
    smoothedArray
  }

  def binaryToString: Array[Byte] => String = (payload: Array[Byte]) => new String(payload)

  def calculateRow(payload: Array[Byte]): String = {
    val values: List[Int] = new String(payload).split(" ")
      .map(_.toInt).toList
    findDirection(values)
  }

  def findDirection(values: List[Int]): String = {
    val deletedAnomalies = deleteAnomalies(values)
    val smoothed: List[Double] = smooth(deletedAnomalies)
    val difference = diff(smoothed)
    log.info(difference.mkString(", "))
    // TODO: Вынести threshold в конфиг
    val anomalies = splitAnomalies(difference, 15)
    log.info(anomalies.mkString(", "))
    // TODO: window вынести в конфиг
    val squashedAnomalies = squashAnomalies(anomalies, difference.length, 6)
    log.info(squashedAnomalies.mkString(", "))
    squashedAnomalies.length match {
      case 0 => {
        log.info("squashedAnomalies = 0")
        humanReadable(getTrend(smoothed))
      }
      case len if len > 0 => {
        log.info(s"squashedAnomalies = $len")
        val delimiters = squashedAnomalies.map(_._1)
        splitSeries(smoothed, delimiters).map {series =>
          humanReadable(getTrend(series))
        }.mkString(",")
      }
      case _ => throw new IllegalStateException("There is not any series for parse")
    }
  }
}
