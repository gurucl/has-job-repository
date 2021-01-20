package com.cnvr.dpl.kafka.producer

import org.apache.spark.sql.SparkSession

object KafkaDataProducer {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("WriteDataFrameToKafka")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq(("iphone", "2007"), ("iphone 3G", "2008"),
      ("iphone 3GS", "2009"),
      ("iphone 4", "2010"),
      ("iphone 4S", "2011"),
      ("iphone 5", "2012"),
      ("iphone 8", "2014"),
      ("iphone 10", "2017"))

    val df = spark.createDataFrame(data).toDF("key", "value")
    /*
      since we are using dataframe which is already in text,
      selectExpr is optional.
      If the bytes of the Kafka records represent UTF8 strings,
      we can simply use a cast to convert the binary data
      into the correct type.

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    */
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test_topic")
      .save()

    println("Successfully Published the messages to kafka...")
  }
}
