package org.kamrus.sensor.streaming

import org.scalatest.FunSpec
import org.scalatest.Matchers._
import SensorProcessingUtils._
import SensorUdf.findDirection
import org.apache.spark.internal.Logging
import org.kamrus.sensor.streaming.states.TrendState

class SensorProcessingUtilsTest extends FunSpec with Logging{
  describe("testing sensor processing utils") {
    it("should smooth signal") {
      val inputSignal = "69 59 72 59 61 61 62 71 73 73 74 75 65 76 76"
        .split(" ")
        .map(_.toInt).toList
      val standard = List(69,
        63.0,
        68.4,
        62.760000000000005,
        61.70400000000001,
        61.281600000000005,
        61.71264,
        67.285056,
        70.7140224,
        72.08560896,
        73.234243584,
        74.2936974336,
        68.71747897344,
        73.086991589376,
        74.8347966357504)
      val smoothed = smooth(inputSignal)
      standard should equal(smoothed)
    }

    it("should delete anomalies") {
      val input = List(69, 63, 68, 62, 61, 61, 61, 84, 76, 74, 73, 73, 74, 68, 73, 74)
      val standard = List(69, 63, 68, 62, 61, 61, 61, 76, 74, 73, 73, 74, 68, 73, 74)

//      val input2 = List(122, 121, 117, 121, 116, 117, 114, 110,
//      109, 108, 108, 108, 111, 110, 109, 104, 103, 107, 105, 104,
//      118, 103, 102, 96, 72, 71, 70, 70, 69, 89, 88, 68, 67, 158,
//      66, 86, 84, 86, 84, 152, 87, 88, 136, 136, 123, 123, 121, 121,
//      120, 120, 119, 119, 119, 118, 116, 116, 116, 119, 117, 115, 115, 115, 145, 115, 113, 113, 113, 113,
//      112, 111, 109, 109, 110, 103, 101, 100, 100, 101, 66, 65)


      deleteAnomalies(input) should equal(standard)
//      deleteAnomalies(input2) should equal(standard2)
    }

    it ("should calculate diff") {
      val inputDiff = List(1.0, 2.0, 4.0, 7.0, 0.0)
      val result = List(1.0, 2.0, 3.0, -7.0)
      diff(inputDiff) should equal(result)
    }

    it ("should squash anomalies") {
      val anomalies: List[(Int, Double)] = List(
        Tuple2(20, -19.166797575466816),
        Tuple2(25, 32.00597199282721),
        Tuple2(56, -21.091755365382284)
      )
      val result = List(Tuple2(25, 32.00597199282721))

      squashAnomalies(anomalies, 58, 6) should equal(result)
    }

    it ("should split anomalies") {
      val inputArray = List(122,
        121.4,
        118.76,
        120.104,
        117.6416,
        117.25664,
        115.302656,
        112.1210624,
        110.24842496,
        108.899369984,
        108.3597479936,
        108.14389919743999)

      val delimiters = List(3, 7)

      val result = List(
        List(122, 121.4, 118.76),
        List(120.104, 117.6416, 117.25664, 115.302656),
        List(112.1210624, 110.24842496, 108.899369984, 108.3597479936, 108.14389919743999)
      )

      splitSeries(inputArray, delimiters) should equal(result)
    }
  }

  it ("should split anomalies") {
    val inputArray = List(
      0.0        ,  -0.6       ,  -2.64      ,   1.344     ,
      -2.4624    ,  -0.38496   ,  -1.953984  ,  -3.1815936 ,
      -1.87263744,  -1.34905498,  -0.53962199,  -0.2158488 ,
      1.71366048,   0.08546419,  -0.56581432,  -3.22632573,
      -1.89053029,   1.64378788,  -0.54248485,  -0.81699394,
      8.07320242,  -5.77071903,  -2.90828761,  -4.76331504,
      -16.30532602,  -7.12213041,  -3.44885216,  -1.37954087,
      -1.15181635,  11.53927346,   4.01570938, -10.39371625,
      6.6425135 ,   1.4570054 ,   1.78280216,  30.71312086,
      4.48524835,   1.79409934,  -0.48236026,  -0.19294411,
      -0.67717764,  -0.27087106,  -0.70834842,  -0.28333937,
      -0.11333575,  -0.6453343 ,  -1.45813372,  -0.58325349,
      -0.2333014 ,   1.70667944,  -0.51732822,  -1.40693129,
      -0.56277252,  -0.22510901,  -1.2900436 ,  -0.51601744,
      -0.20640698,  -0.08256279,  -0.63302512,  -0.85321005,
      -1.54128402,  -0.61651361,   0.35339456,  -4.05864218,
      -2.82345687,  -1.72938275,  -0.6917531 , -21.27670124)

    val result = List((24, -16.30532602),
      (35, 30.71312086),
      (67, -21.27670124))

    splitAnomalies(inputArray, 15) should equal(result)
  }

  it ("should define trend") {
    val input = List(
      121.4,
      118.76,
      120.104,
      117.6416,
      117.25664,
      115.302656,
      112.1210624,
      110.24842496,
      108.899369984)

    getTrend(input) should equal(TrendState.Decrease)
  }

  it ("should find two in") {
    val input: List[Int] = List(122, 121, 117, 121, 116, 117, 114, 110, 109,
      108, 108, 108, 111, 110, 109, 104, 103, 107, 105, 104, 118,
      103, 102, 96, 72, 71, 70, 70, 69, 89, 88, 68, 67, 158, 66, 86, 84,
      86, 84, 152, 87, 88, 136, 136, 123, 123, 121, 121, 120, 120, 119,
      119, 119, 118, 116, 116, 116, 119, 117, 115, 115, 115, 145, 115,
      113, 113, 113, 113, 112, 111, 109, 109, 110, 103, 101, 100, 100, 101, 66, 65)

    println(findDirection(input))
  }
}