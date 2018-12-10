package org.kamrus.sensor.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object JobExitCode {
  final val SUCCESS = 0
  final val UNKNOWN_ERROR = 1
  final val OPTIONS_ERROR = 2
  final val JOB_ERROR = 3
}

class JobError(message: String, cause: Option[Throwable] = None) extends RuntimeException(message, cause.orNull) {
  def this(message: String, cause: Throwable) = this(message, Some(cause))
}

final class JobExit(
                     val exitCode: Int,
                     additionalMessage: Option[String] = None,
                     cause: Option[Throwable] = None)
  extends RuntimeException(
    JobExit.compileMessage(exitCode, additionalMessage, cause),
    cause.orNull
  )

object JobExit {
  def compileMessage(exitCode: Int, additionalMessage: Option[String], cause: Option[Throwable]): String = {
    val lines: List[String] = List(
      s"[[exit_code:$exitCode]]"  // don't change this line format
    )

    val withAdditionalMessage = additionalMessage match {
      case Some(message) => additionalMessage +: lines
      case _ => lines
    }

    val withCause = cause match {
      case Some(error: Throwable) =>
        withAdditionalMessage ++ List(
          s"Exception raised: ${error.getMessage}",
          error.getStackTrace
        )
      case _ => withAdditionalMessage
    }
    withCause.mkString("\n")
  }
}

trait StreamingJob[Opts <: JobOptions] extends Logging {

  type OptsType = Opts

  def jobName: String

  def buildOptions(args: Seq[String]): Opts

  def exec(options: Opts)(implicit session: SparkSession): Int

  def exit(code: Int): Unit = exit(code, None)

  def exit(code: Int, exception: Option[Throwable] = None): Unit = {
    if (code != 0) {
      log.info(s"Exit with code $code")
      throw new JobExit(code, cause = exception)
    } else {
      log.info(s"Don't call System.exit(), so 0 exit code will be used")
    }
  }

  def postprocessSparkSessionBuilder(sessionBuilder: SparkSession.Builder, options: Opts): Unit = {}
  def stopSparkSession(session: SparkSession): Unit = session.stop()

  protected def buildAndVerifyOptions(args: Seq[String]): Opts = {
    val options = buildOptions(args)
    options.exit = exit
    options.verify()
    options
  }

  def main(args: Array[String]): Unit = {
    val options = buildAndVerifyOptions(args)
    implicit val session: SparkSession = buildSparkSession(options)
    val (exitCode: Int, cause: Option[Throwable]) = try {
      log.info(s"Start $jobName")
      (exec(options), None)
    } catch {
      case e: JobError =>
        log.error("Job error", e)
        (JobExitCode.JOB_ERROR, Some(e))
      case e: Throwable =>
        log.error("Unexpected error", e)
        (JobExitCode.UNKNOWN_ERROR, Some(e))
    } finally {
      log.info("Stop spark session")
      stopSparkSession(session)
    }
    log.info(s"$jobName has been done. exit code: $exitCode")

    exit(exitCode, cause)
  }

  private def buildSparkSession(options: Opts): SparkSession = {
    val sparkSessionBuilder = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.session.timeZone", options.timezone())
      .master("local[*]")
      .config("spark.driver.host", "localhost")

    postprocessSparkSessionBuilder(sparkSessionBuilder, options)
    sparkSessionBuilder.getOrCreate()
  }
}

