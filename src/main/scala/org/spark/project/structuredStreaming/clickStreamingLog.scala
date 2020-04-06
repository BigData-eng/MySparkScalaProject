
package org.spark.project.structuredStreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import sparkStreamingExample.Utilities._

object clickStreamingLog extends App {

  val spark = SparkSession.builder().appName("StructuredNetworkCount")
    .master("local[*]")
    .getOrCreate()
  val lines = spark.readStream.format("socket")
    .option("host","ec2-52-66-243-42.ap-south-1.compute.amazonaws.com")
    .option("port", 9999)
    .load()
  setupLogging()  // setting log info
import spark.implicits._
  val words = lines.as[String].flatMap(_.split(" "))

  val wordsCount=words.groupBy("value").count()

  val query=wordsCount.writeStream
    .outputMode("Complete")
    .format("console")
    .start()


  query.awaitTermination()




}
