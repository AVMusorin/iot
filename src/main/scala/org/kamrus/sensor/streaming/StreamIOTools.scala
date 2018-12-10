package org.kamrus.sensor.streaming

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.spark.sql._

object StreamIOTools {
  def readFromFile(path: String, maxFilePerTrigger: Int = 1)
                  (implicit spark: SparkSession): DataFrame = {
    spark.readStream
      .format("text")
      .option("maxFilesPerTrigger", maxFilePerTrigger)
      .load(path)
  }

  def readFromKafka(host: String, port: String, topic: String)
                   (implicit spark: SparkSession): DataFrame = {
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", s"$host:$port")
      .option("failOnDataLoss", "false")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }


  def writeToConsole(stream: DataFrame)(implicit spark: SparkSession): DataStreamWriter[Row] = {
    stream.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
  }

  def writeToElastic(stream: DataFrame)(implicit spark: SparkSession): DataStreamWriter[Row] = {
    val host = "localhost"
    val indexName = "sensor"
    val indexType = "test"
    stream.writeStream
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only","true")
      .option("es.nodes", host) // по дефолту будет писать на 9200 порт
      .outputMode("Append")
      .option("checkpointLocation", "/tmp/")
      .option("es.resource", s"$indexName/$indexType")
  }
}
