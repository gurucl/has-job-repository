package com.cnvr.dpl.kafka.consumer

import java.util.UUID

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object KafkaSparkStreaming {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]").setAppName("KafkaSparkStreaming")
      .set("spark.streaming.stopGracefullyOnShutdown","true")
      .set("spark.streaming.kafka.maxRatePerPartition","2")
    val ssc = new StreamingContext(conf, Seconds(2))

    ssc.sparkContext.setLogLevel("ERROR")

    val preferredHosts = LocationStrategies.PreferConsistent

    val topics = List("test_topic")

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ("consumer-11" + UUID.randomUUID().toString),
      "auto.offset.reset" -> "earliest"
   // "enable.auto.commit" -> (false: java.lang.Boolean)
    )

   // val offsets = Map(new TopicPartition("test_topic", 0) -> 0L)

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      Subscribe[String, String](topics, kafkaParams)) //, offsets))

    dstream.foreachRDD { rdd =>

      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }

      //  rdd.map(x => (x.topic(), x.partition(), x.offset(), x.key(), x.value(), x.timestamp(), x.timestampType() )).foreach(println)

      rdd.foreachPartition(partition => {
        partition.foreach(x => {

         println (x.topic(), x.partition(), x.offset(), x.key(), x.value(),
            x.timestamp(), x.timestampType())

        })
      })

    }

    ssc.start

    // the above code is printing out topic details every 5 seconds
    // until you stop it.

    ssc.awaitTermination()
   // ssc.stop(stopSparkContext = false)

    println("Application has ended successfully...")

  }
}
