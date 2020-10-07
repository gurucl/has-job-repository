package com.cnvr.dpl.core

import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import io.confluent.kafka.serializers.KafkaAvroDeserializer


object KafkaAvroConsumerSample {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    var counter = 0

    //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node101.ord.logs.crm.cnvr.net:9092")
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node101.ord.logs.crm.cnvr.net:9092,node201.ord.logs.crm.cnvr.net:9092,node301.ord.logs.crm.cnvr.net:9092")
    props.put("schema.registry.url", "http://ord-schema-registry-logst2.cnvr.in")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    // props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
   // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
   // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    //props.put("compression.type", "snappy")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "prd.cnvrcorp.dpl.logs")

    val topic = "ContentUrl"

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList(topic))

    try while (true) {
      val records = consumer.poll(1000)
      import scala.collection.JavaConversions._
      for (record <- records) {
        println(s"topic = ${record.topic()}, partition = ${record.partition()}, offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}\n")
        counter = counter+1
        println(s"Successfully read total :$counter records from Kafka topic: $topic \n")
      }
    }
    finally consumer.close()
    println("Closed the Kafka Consumer Successfully...")
  }





}
