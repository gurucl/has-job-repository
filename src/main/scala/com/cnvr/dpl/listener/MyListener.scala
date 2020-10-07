package com.cnvr.dpl.listener

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, SparkListenerTaskStart}

class MyListener(sc:SparkContext) extends SparkListener {

  var tasksCount = 0
  var stageCount = 0

  val applicationId = sc.getConf.getAppId
  val applicationName = sc.getConf.get("spark.app.name","default_app_name")
  val appUserName = sc.sparkUser
  var startTime:Long = System.currentTimeMillis()

  case class AppVals(appID: String, AppName: String, AppDuration:Long, AppUserName: String, numberOfStages:Int, numberOfTasks:Int)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

    println("Application has started...")

  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    val appDuration = (System.currentTimeMillis()- startTime)/1000
    val appInfo = AppVals(applicationId, applicationName, appDuration, appUserName, stageCount, tasksCount )
    println("Application has ended...")

    println(appInfo.toString)
    println(s"Stages Count :${stageCount}")
    println(s"Tasks Count :${tasksCount}")

  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    println("Stage has been started...")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {


    stageCount += 1
    println("Stage has been completed...")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    println("Job has started...")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {


    println("Job has ended...")

  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {

    println("Task has started...")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCount +=1
    println("Task has ended...")

  }

}
