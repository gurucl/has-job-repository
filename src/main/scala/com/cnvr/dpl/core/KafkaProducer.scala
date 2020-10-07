package com.cnvr.dpl.core

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.Future

import scala.io.Source
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.Logger
import KafkaProducer._
import com.cnvr.dpl.listener.AppVals
import com.cnvr.dpl.util.AvroUtils
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata}

object KafkaProducer {

  private var TOPIC: String = _

  private var schema_loc: String = _

  private var bootstrapAddress: String = _

  private var schema_url: String = _

  private var keystore_loc: String = _

  private var truststore_loc: String = _

  private var keypassword: String = _

  private var keystorepassword: String = _

  private var truststorepassword: String = _


  def buildKafkaConfig(): java.util.Properties = {
   // val props = new java.util.Properties()
    initialize()

    val props = new Properties()

    props.load(new FileInputStream("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/dev/kafka.properties"))

    //val reader = Source.fromURL(getClass.getResource("kafka.properties")).bufferedReader()
    //props.load(reader)

    //props.put("metadata.broker.list", bootstrapAddress)
    //props.put("bootstrap.servers", bootstrapAddress)

    //props.put("key.serializer", classOf[org.apache.kafka.common.serialization.StringSerializer])
    //props.put("value.serializer", classOf[io.confluent.kafka.serializers.KafkaAvroSerializer])
    //props.put("schema.registry.url", schema_url)
//    props.put("producer.type", "async")
//    props.put(ProducerConfig.ACKS_CONFIG, "all")
//    props.put(ProducerConfig.RETRIES_CONFIG, "3")
//    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
//    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
//    // prop.put(ProducerConfig.LINGER_MS_CONFIG, "1")
//    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
//    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000")
    //props.put(ProducerConfig.ACKS_CONFIG, "all")
    //props.put("security.protocol", "SSL")
    //props.put("ssl.truststore.location", truststore_loc)
    //props.put("ssl.truststore.password", truststorepassword)
    //props.put("ssl.keystore.location", keystore_loc)
    //props.put("ssl.keystore.password", keypassword)
    //props.put("ssl.key.password", keypassword)

    println(props)
    props
  }


  def sendToKafkaWithNewProducer(appval: AppVals) = {
    var producer: org.apache.kafka.clients.producer.KafkaProducer[String, GenericRecord] = null
    try {
      producer = createProducer()

      val value = AvroUtils.serialize(appval)

      val data = new ProducerRecord[String, GenericRecord](TOPIC, "jobmetrics", value)

      //producer.send(data)

      try{

        val metadata  = producer.send(data, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
           // println("User Defined onCompletion method has been called...")
            if (exception != null) {

              println(s"Cannot publish to $TOPIC. Caused by: ${exception.getMessage}")
            }
          }
        }).get()

        if (metadata.hasOffset){
          println(s"Record has been successfully published to the topic: $TOPIC")
        }else{
          println(s"Error while publishing record to the topic...")
        }

//        if (metadata != null){
//          println(s" Publish success status: ${metadata.hasOffset}")
//        }else{
//          println(s" Metadata is null...")
//        }

      }catch{

        case e: Exception => println(s"Cannot publish to $TOPIC. Caused by: ${e.getMessage}")
          e.printStackTrace()
      }








      //      val p = Promise[(RecordMetadata, Exception)]()
      //      producer.send(data, new Callback {
      //        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      //
      //          println(s"On completion has been called..")
      //
      //          p.success((metadata, exception))
      //
      //          if (exception!=null){
      //            println(s"Exception has occured : ${exception.getMessage}")
      //            exception
      //          }
      //
      //
      //        }
      //      })
      //
      //      if (p !=null) {
      //
      //        println(s" is Completed:  ${p.isCompleted}")
      //        println(s" String value:  ${p.toString}")
      //        println(s" is Completed:  ${p.future.toString}")
      //
      //      }




      producer.close()
    } finally {
      if (producer != null) {
        producer.close()
      }
    }
  }

  def createProducer(): org.apache.kafka.clients.producer.KafkaProducer[String, GenericRecord] = {
    new org.apache.kafka.clients.producer.KafkaProducer[String, GenericRecord](buildKafkaConfig())
  }


  def flush(): Unit = {

  }

  def close(): Unit = {
  }


  def initialize(): Unit = {
    //bootstrapAddress=prop.getProperty("bootstrapAddress")
    //schema_url=prop.getProperty("schema_url")
    //keypassword=prop.getProperty("keypassword")
    //truststorepassword=prop.getProperty("truststorepassword")
    //truststore_loc=prop.getProperty("truststore_loc")
    //keystore_loc=prop.getProperty("keystore_loc")
    //TOPIC=prop.getProperty("TOPIC")

    val prop=new java.util.Properties()
    val reader = Source.fromURL(getClass.getResource("/dev/kafka.properties")).bufferedReader()
    prop.load(reader)

    //  bootstrapAddress="kaas-test-ctc-a.optum.com:443"
    //  schema_url="http://kaas-test-schema-registry-a.optum.com"
    //  keypassword="aAEVmYtiDyrjlAJMfl87fg"
    //  truststorepassword="IKf111xwH5zkuHBgdb5lkg"
    //  truststore_loc=""
    //  keystore_loc=""
    //  TOPIC=""

    // bootstrapAddress = prop.getProperty("bootstrapAddress")
    //schema_url = prop.getProperty("schema_url")
    //keypassword = prop.getProperty("keypassword")
    //truststorepassword = prop.getProperty("truststorepassword")
    //truststore_loc = prop.getProperty("truststore_loc")
    //keystore_loc = prop.getProperty("keystore_loc")
    TOPIC = prop.getProperty("TOPIC")

    // println(keystore_loc)
    //println(truststore_loc)
  }

}

