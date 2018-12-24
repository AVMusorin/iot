package org.kamrus.sensor.streaming

import breeze.interpolation.LinearInterpolator

import scala.collection.mutable.ArrayBuffer
import scala.math.abs
import breeze.linalg.Vector
import org.kamrus.sensor.streaming.states.{HumanState, TrendState}
import org.kamrus.sensor.streaming.states.TrendState._
import states.HumanState._


object SensorProcessingUtils {

  /**
    * Функция удаляет выбросы
    * @param values
    * @param maxAnomaly максимальный размер выброса
    * @return
    */
  // TODO: переделать, не нужно смотреть на удаленное значение из входного списка
  def deleteAnomalies(values: List[Int], maxAnomaly: Int = 15): List[Int] = {
    val size = values.length
    val newArray = ArrayBuffer.empty[Int]
    newArray += values.head
    for (i <- 1 until size) {
      if (i + 1 < size) {
        val average = (values(i - 1) + values(i + 1)) / 2
        if (abs(values(i) - average) < maxAnomaly) {
          newArray += values(i)
        }
      } else newArray += values(i)
    }
    newArray.toList
  }

  def diff(values: List[Double]): List[Double] = {
    val difference = ArrayBuffer.empty[Double]
    for (i <- values.indices zip(1 until values.length)) {
      difference += values(i._2) - values(i._1)
    }
    difference.toList
  }

  /**
    * @param values
    * @param threshold - порог отсечения
    * @return
    */
  def splitAnomalies(values: List[Double], threshold: Int): List[(Int, Double)] = {
    val anomalies = ArrayBuffer.empty[(Int, Double)]
    for (i <- values.indices) {
      if (math.abs(values(i)) >= threshold) {
        anomalies += Tuple2(i, values(i))
      }
    }
    anomalies.toList
  }

  def squashAnomalies(values: List[(Int, Double)], length: Int, window: Int): List[(Int, Double)] = {
    val valid = ArrayBuffer.empty[(Int, Double)]
    val first = ArrayBuffer(values: _*)
    val second = ArrayBuffer(values.slice(1, values.length): _*)
    second += Tuple2(0, 0.0)
    val difference = first.zip(second).map {
      case (x, y) => math.abs(x._1 - y._1)
    }
    for (i <- values.zip(difference)) {
      if (i._2 >= window && i._1._1 < length - window) {
        valid += i._1
      }
    }
    valid.toList
  }

  def smooth(values: List[Int], alpha: Double = 0.6): List[Double] = {
    if (values.isEmpty) {
      List.empty[Double]
    } else {
      val smoothedArray = ArrayBuffer.empty[Double]
      smoothedArray += values.head.toDouble
      for (i <- 1 until values.length) {
        smoothedArray += alpha * values(i) + (1 - alpha) * smoothedArray(i - 1)
      }
      smoothedArray.toList
    }
  }

  def splitSeries(values: List[Double], delimiters: List[Int]): List[List[Double]] = {
    val splits = ArrayBuffer.empty[List[Double]]
    var start = -1
    var stop = -1
    for (i <- delimiters.indices) {
      if (i == 0) {
        start = 0
      } else {
        start = delimiters(i - 1)
      }
      stop = delimiters(i)
      splits += values.slice(start, stop)
      if (i == delimiters.length - 1) {
        start = stop
        stop = values.length
        splits += values.slice(start, stop)
      }
    }
    splits.toList
  }

  /**
    * Определяет тренд функции
    * @param values значения функции
    * @return -1 - вышел, 1 - зашел, 0 - неизвестно
    */
  def getTrend(values: List[Double]): Int = {
//    Intval linearInterpolator = LinearInterpolator(
//      Vector(values.indices.toList.map(_.toDouble): _*), Vector(values.toVector: _*))
    var sum_x = 0.0
    var sum_y = 0.0
    var sum_xy = 0.0
    var sum_x2 = 0.0
    var n: Int = values.size
    for (i <- 0 until n) {
      sum_x += i
      sum_y += values(i)
      sum_xy += i * values(i)
      sum_x2 += i * i
    }
    val k = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    val b = (sum_y - k * sum_x) / n
    if (k > 0) {
      Increase
    } else if (k < 0) {
      Decrease
    } else {
      TrendState.Undefined
    }
  }

  def humanReadable(value: Int): String = {
    value match {
      case Increase => Out
      case TrendState.Decrease => In
      case _ => HumanState.Undefined
    }
  }
}
