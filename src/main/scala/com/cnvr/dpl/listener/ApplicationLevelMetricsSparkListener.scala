package com.cnvr.dpl.listener

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}

import scala.collection.mutable

class ApplicationLevelMetricsSparkListener extends SparkListener {

  val taskMetricsBuffer = new scala.collection.mutable.ListBuffer[TaskMetrics]()
  val stageMetricsBuffer = new scala.collection.mutable.ListBuffer[StageMetrics]()
  val jobIds = new scala.collection.mutable.ListBuffer[Int]

  val stageToJobMapping = new mutable.HashMap[Int, Int]()
  val jobMetricsBuffer = new scala.collection.mutable.ListBuffer[JobMetrics]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println((taskEnd.stageId, taskEnd.taskInfo.taskId, taskEnd.taskMetrics.toString))

    val taskId = taskEnd.taskInfo.taskId
    val stageId = taskEnd.stageId
    val taskMetrics = taskEnd.taskMetrics
    val taskInputMetrics = taskMetrics.inputMetrics
    val taskOutputMetrics = taskMetrics.outputMetrics

    /*val taskShuffleReadMetrics = taskEnd.taskMetrics.shuffleReadMetrics
    val taskShuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics*/

    val metricsForTask = TaskMetrics(
      stageId,
      taskId,
      taskInputMetrics.bytesRead,
      taskInputMetrics.recordsRead,
      taskOutputMetrics.bytesWritten,
      taskOutputMetrics.recordsWritten,
      taskMetrics.executorRunTime,
      taskMetrics.jvmGCTime
    )

    /*taskShuffleReadMetrics.recordsRead
    taskShuffleReadMetrics.totalBytesRead
    taskShuffleWriteMetrics.bytesWritten
    taskShuffleWriteMetrics.recordsWritten
    taskEnd.taskMetrics.diskBytesSpilled
    taskEnd.taskMetrics.executorCpuTime
    taskEnd.taskMetrics.executorRunTime
    taskEnd.taskMetrics.jvmGCTime
    taskEnd.taskMetrics.resultSize
    taskEnd.taskMetrics.memoryBytesSpilled*/
    println(metricsForTask)

    taskMetricsBuffer += metricsForTask
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    val stageInfo = stageCompleted.stageInfo

    val stageId = stageInfo.stageId
    val jobId = stageToJobMapping(stageId)
    val stageName = stageInfo.name
    val parentIds = stageInfo.parentIds
    val executorRunTime = stageInfo.taskMetrics.executorRunTime

    val metricsForStage = StageMetrics(stageId, jobId, stageName, parentIds, executorRunTime)
    stageMetricsBuffer += metricsForStage
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    jobStart.stageIds.foreach(x => stageToJobMapping += (x -> jobId))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val aggregatedExecutorRunTime = stageMetricsBuffer.toList.filter(x => x.jobId == jobId).map(x => x.executorRunTime).sum

    jobMetricsBuffer += JobMetrics(jobId, aggregatedExecutorRunTime)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    //taskMetricsBuffer.toList.foreach(println)

    taskMetricsBuffer.toList.map(x => (x.stageId, x.executorRunTime)).groupBy(x => x._1).map(x => (x._1, x._2.map(y => y._2).sum)).foreach(println)
    stageMetricsBuffer.toList.foreach(println)
    jobIds.toList.foreach(println)

    jobMetricsBuffer.toList.foreach(println)
  }

}

case class TaskMetrics(stageId: Int, taskId: Long, bytesRead: Long, recordsRead: Long, bytesWritten: Long, recordsWritten: Long, executorRunTime: Long, jvmGCTime: Long)

case class StageMetrics(stageId: Int, jobId: Int, stageName: String, parentIds: Seq[Int], executorRunTime: Long)

case class JobMetrics(jobId: Long, jobRunTime: Long)
