package org.kamrus.sensor.streaming.states

object TrendState extends Enumeration {
  type Trend = Int
  val Increase: Trend = 1
  val Decrease: Trend = -1
  val Undefined: Trend = 0
}
