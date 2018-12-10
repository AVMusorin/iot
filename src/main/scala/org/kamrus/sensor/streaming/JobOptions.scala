package org.kamrus.sensor.streaming

import org.apache.spark.internal.Logging
import org.rogach.scallop.exceptions.ScallopException
import org.rogach.scallop.{ScallopConf, ScallopOption}

private case object JobAborted extends Exception

class JobOptions(arguments: Seq[String], jobName: String, optionsErrorCode: Int = JobExitCode.OPTIONS_ERROR)
  extends ScallopConf(arguments) with Logging {

  val localDebugRun: ScallopOption[Boolean] = toggle(default = Some(false))
  val timezone: ScallopOption[String] = opt(required = false, default = Some("Europe/Moscow"))
  var exit: Int => Unit = code => System.exit(code)

  override def onError(e: Throwable): Unit = e match {
    case error: ScallopException =>
      log.error(s"Error: ${error.getMessage}\n")
      printHelp()
      exit(optionsErrorCode)
      throw new RuntimeException("Exiting")  // exit should throw execution
    case other => super.onError(other)
  }
}
