package com.cnvr.dpl.kafka.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object KafkaStreamReader {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("KafkaBatchReader")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test_topic")
      .option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("fetchOffset.numRetries", 3)
      .option("fetchOffset.retryIntervalMs", 10)
      .option("maxOffsetsPerTrigger", 5)
      .load()

    df.printSchema()

    val df2 = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)", "topic")

    df2.writeStream
      .format("console")
      .outputMode("append")
      // .trigger(Trigger.ProcessingTime("10 seconds"))
      .trigger(Trigger.Once())
      .start()
      .awaitTermination()


    //    val schema = new StructType()
//      .add("id",IntegerType)
//      .add("phone_model",StringType)
//      .add("year",StringType)
//      .add("topic_name",StringType)
//      .add("dob_year",IntegerType)
//      .add("dob_month",IntegerType)
//      .add("gender",StringType)
//      .add("salary",IntegerType)

//    val personDF = df2.select(from_json(col("value"), schema).as("data"))
//      .select("data.*")


  }
}
