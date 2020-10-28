package com.cnvr.dpl.core

import java.util.{Properties, UUID}

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.{Schema}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.avro.{SchemaConverters, from_avro}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}


object StructuredStreamingApp {

  private val topic = "my-topic"
  private val kafkaUrl = "localhost:9092"
  private val schemaRegistryUrl = "http://localhost:8081"

  private val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
  private val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  private val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
  private var sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Structured_Streaming_App").master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.udf.register("deserialize", (bytes: Array[Byte]) => DeserializerWrapper.deserializer.deserialize(bytes) )

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("schema.registry.url", schemaRegistryUrl)
      .option("subscribe", topic)
       .option("startingOffsets", "earliest")
    //  .option("endingOffsets", "")
      .option("failOnDataLoss", "false")
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("fetchOffset.numRetries", 3)
      .option("fetchOffset.retryIntervalMs", 10)
      .option("maxOffsetsPerTrigger", 100)
      .option("group.id", "consumer-11" + UUID.randomUUID().toString)
     // .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     // .option("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
     // .option("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
      .option("enable.auto.commit", "true")
      .load

    df.printSchema()

   // df.show(false)

    //    val dataSet: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .as[(String, String)]

    val valueDataFrame = df.selectExpr("CAST (key as string)", """deserialize(value) AS message""",

                                           "topic", "partition", "offset", "timestamp", "timestampType" )

    val formattedDataFrame = valueDataFrame.select(
      col("key"), from_json(col("message"), sparkSchema.dataType).alias("parsed_value"),
      col("topic"), col("partition"), col("offset"), col("timestamp"), col("timestampType") )
      .select("key","parsed_value.*", "topic", "partition", "offset", "timestamp", "timestampType")

    formattedDataFrame.printSchema()

   // formattedDataFrame.show(false)


    formattedDataFrame
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()


  }

  object DeserializerWrapper {
    val deserializer = kafkaAvroDeserializer
  }

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
      genericRecord.toString
    }
  }

}
