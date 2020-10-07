package com.cnvr.dpl.core

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.kafka.clients.Metadata
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.Future


object KafkaProducerSample {

  def main(args: Array[String]): Unit = {

    val props = new Properties()

    props.load(new FileInputStream(args(0)))

    val topicName = props.getProperty(CommonConfig.TOPIC_NAME)

    println(s"Publishing to the topic: $topicName")

    val producer = getKafkaProducer(props)

   // val acks: Future[Metadata]=

      producer.send(new ProducerRecord[String, String](topicName,"key1", "My Name is Sheela"))

    println("Successully published the mesage to kakfa")

  }


  def getKafkaProducer(prop: Properties):KafkaProducer[String, String]={

    val props = new Properties()

    props.put(CommonConfig.BOOTSTRAP_SERVERS, prop.getProperty(CommonConfig.BOOTSTRAP_SERVERS))
    props.put(CommonConfig.SCHEMA_REGISTRY_URL, prop.getProperty(CommonConfig.SCHEMA_REGISTRY_URL))
    props.put(CommonConfig.KEY_SERIALIZERS, prop.getProperty(CommonConfig.KEY_SERIALIZERS))
    props.put(CommonConfig.VALUE_SERIALIZERS, prop.getProperty(CommonConfig.VALUE_SERIALIZERS))
    props.put(CommonConfig.ACKS, prop.getProperty(CommonConfig.ACKS))

    println("Succesfully created the Kafka Producer")

    new KafkaProducer[String, String](prop)
  }
}
