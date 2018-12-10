package org.kamrus.sensor.streaming.states

object HumanState extends Enumeration {
  type HumanState = String
  val In = "in"
  val Out = "out"
  val Undefined = "undefined"
}
