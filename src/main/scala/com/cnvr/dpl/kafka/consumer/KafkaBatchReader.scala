package com.cnvr.dpl.kafka.consumer

import org.apache.spark.sql.SparkSession

object KafkaBatchReader {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("KafkaBatchReader")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test_topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    // val df = spark
    // .read()
    // .format("kafka")
    // .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
    // .option("subscribe", "topic1,topic2")
    // .option("startingOffsets", "{\"topic1\":{\"0\":23,\"1\":-2},\"topic2\":{\"0\":-2}}")
    // .option("endingOffsets", "{\"topic1\":{\"0\":50,\"1\":-1},\"topic2\":{\"0\":-1}}")
    // .load();

    df.printSchema()

    val df2 = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)", "topic")
    df2.show(false)
  }
}
