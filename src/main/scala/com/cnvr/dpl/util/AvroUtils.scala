package com.cnvr.dpl.util

import java.io.ByteArrayOutputStream
import java.util

import com.cnvr.dpl.listener.AppVals
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import scala.collection.JavaConversions._
import scala.io.Source

object AvroUtils {

  case class Operation(
                        appId: String,
                        appName: String,
                        groups: String,
                        subGroups: String,
                        jobType: String,
                        appStartTime: String,
                        appEndTime: String,
                        appDuration: Int,
                        cpuTime: Int,
                        runTime: Int,
                        memoryUsage: Int,
                        appStatus: String,
                        appResult: String,
                        failureREason: String,
                        recordCount: String,
                        numexecutors: String,
                        executorcores: String,
                        executormemory: String
                      )


  // val avroSchema = Source.fromInputStream(getClass.getResourceAsStream("src/main/resources/metrics.avsc")).mkString
  val avroSchema = Source.fromFile("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/dev/metrics.avsc").getLines.mkString
  val schema = new Schema.Parser().parse(avroSchema)

  val reader = new GenericDatumReader[GenericRecord](schema)
  val writer = new GenericDatumWriter[GenericRecord](schema)

  def serialize(operation: AppVals): GenericRecord= {

    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)

    val avroRecord = new GenericData.Record(schema)


    avroRecord.put("appId", operation.appId)
    avroRecord.put("appName", operation.appName)
    avroRecord.put("groups", operation.groups)
    avroRecord.put("subGroups", operation.subGroups)
    avroRecord.put("jobType", operation.jobType)
    avroRecord.put("appStartTime", operation.appStartTime)
    avroRecord.put("appEndTime", operation.appEndTime)
    avroRecord.put("appDuration", operation.appDuration)
    avroRecord.put("cpuTime", operation.cpuTime)
    avroRecord.put("runTime", operation.runTime)
    avroRecord.put("memoryUsage", operation.memoryUsage)
    avroRecord.put("appStatus", operation.appStatus)
    avroRecord.put("appResult", operation.appResult)
    avroRecord.put("failureREason", operation.failureREason)
    avroRecord.put("recordCount", operation.recordCount)
    avroRecord.put("numexecutors", operation.numexecutors)
    avroRecord.put("executorcores", operation.groups)
    avroRecord.put("executormemory", operation.executormemory)
    avroRecord.put("queueName", operation.queueName)
    avroRecord.put("sparkUserName", operation.sparkUserName)

    //    writer.write(avroRecord, encoder)
    //    encoder.flush
    //    out.close
    //    out.toByteArray

    avroRecord

  }

  def deserialize(bytes: Array[Byte]): AppVals = {

    val decoder = DecoderFactory.get.binaryDecoder(bytes, null)
    val record = reader.read(null, decoder)


    val appId=record.get("appId").toString
    val appName= record.get("appName").toString
    val groups= record.get("groups").toString
    val subGroups= record.get("subGroups").toString
    val jobType =record.get("jobType").toString
    val appStartTime = record.get("appStartTime").toString
    val appEndTime = record.get("appEndTime").toString
    val appDuration =record.get("appDuration").asInstanceOf[Int]
    val cpuTime = record.get("cpuTime").asInstanceOf[Int]
    val runTime =record.get("runTime").asInstanceOf[Int]
    val memoryUsage = record.get("memoryUsage").asInstanceOf[Int]
    val appResult = record.get("appResult").toString
    val failureREason = record.get("failureREason").toString
    val recordCount = record.get("recordCount").toString
    val numexecutors = record.get("numexecutors").toString
    val executorcores = record.get("executorcores").toString
    val executormemory= record.get("executormemory").toString
    val appStatus = record.get("appStatus").toString
    val queueName = record.get("queueName").toString
    val sparkUserName = record.get("sparkUserName").toString
    val numberOfTasks = record.get("numberOfTasks").asInstanceOf[Int]
    val numberOfStages = record.get("numberOfStages").asInstanceOf[Int]
    val numberOfJobs = record.get("numberOfJobs").asInstanceOf[Int]

    val inputBytes = record.get("inputBytes").asInstanceOf[Long]
    val outputBytes = record.get("outputBytes").asInstanceOf[Long]
    val inputRecords = record.get("inputRecords").asInstanceOf[Long]
    val outputRecords = record.get("outputRecords").asInstanceOf[Long]


    AppVals(appId,appName,groups,subGroups,jobType,appStartTime,appEndTime,appDuration,cpuTime,runTime,memoryUsage,appStatus,
      appResult,failureREason,recordCount,numexecutors,executorcores,executormemory,queueName,sparkUserName, numberOfTasks, numberOfStages , numberOfJobs,
      inputBytes, outputBytes, inputRecords, outputRecords
    )
  }

}
