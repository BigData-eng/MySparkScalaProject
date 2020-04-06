package org.spark.project.kafkaStreaming

import common.Utilities.setupLogging
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark_kafka_integration {
  def main(args: Array[String]) {

    setupLogging()
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSpark Discretized Stream")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("Message")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)

    )

    val streamdata=stream.map(record =>( record.value))

        val words= streamdata.foreachRDD{
         rdd =>
           val words= rdd.flatMap(_.split(","))


//             toString().split(",")
            val count= words.map(w=> (w,1)).reduceByKey(_+_)
            count.foreach(println)

        }
//    streamdata.foreachRDD { rdd => {
//
//      import org.apache.spark.sql.SparkSession
//      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
//      import spark.implicits._
//
//
//
//      rdd.collect().foreach(println)
//    }
//    }
    ssc.start
    ssc.awaitTermination()


  }
}
