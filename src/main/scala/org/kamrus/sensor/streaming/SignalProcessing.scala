package org.kamrus.sensor.streaming


import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import StreamIOTools._
import SensorUdf._

case class Signal(value: Array[Int])

trait SignalProcessing
  extends StreamingJob[SignalProcessingOptions]
  with Logging {

  override val jobName = "Signal Processing"

  override def buildOptions(args: Seq[String]): SignalProcessingOptions = new SignalProcessingOptions(args, jobName)

  final override def exec(options: SignalProcessingOptions)(implicit spark: SparkSession): Int = {
    import spark.implicits._
    log.info(s"Start stream from kafka")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    try {
      val stream: DataFrame = readFromKafka("localhost", "9092", "sensor")
      val signals = stream
        .withColumn("value", binaryToStringUdf($"value"))
        .filter(length($"value") > 0)
        .withColumn("result", calculateRowUdf($"value"))
        .select($"value", explode(split($"result", ",")) as "result")

      val query = writeToElastic(signals).start()
      query.awaitTermination()
      JobExitCode.SUCCESS
    } catch {
      case JobAborted =>
        JobExitCode.JOB_ERROR
    } finally {
      log.info(s"Stop stream from kafka")
    }
  }
}

object SignalProcessingJob extends SignalProcessing

class SignalProcessingOptions(arguments: Seq[String], jobName: String)
extends JobOptions(arguments, jobName) {
  import org.rogach.scallop.{ScallopOption => Opt}
  val inputPath: Opt[String] = trailArg(required = true)
}