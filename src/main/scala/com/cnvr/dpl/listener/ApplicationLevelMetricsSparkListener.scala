//package com.cnvr.dpl.listener
//
//import java.io.PrintWriter
//import java.text.SimpleDateFormat
//import java.util
//import java.util.{Date, Properties}
//
//import scala.collection.mutable.ListBuffer
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.spark.{ExceptionFailure, Success, TaskEndReason, TaskKilled}
//import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, TaskLocality}
//import org.apache.spark.sql.SparkSession
//import org.json.JSONObject
//import org.apache.log4j.Logger
//import org.codehaus.jackson.map.ObjectMapper
//
//import scala.collection.JavaConversions.asScalaIterator
//
//
//
////  ************************ Case Class Schema for Metrics Capturing   ************************
//
//case class TaskMetrics(jobId: Int, stageId: Int, taskId: Long, launchTime: String, finishTime: String,
//                       duration: Long, executorId: String, host: String, taskLocality: Int,
//                       speculative: Boolean, gettingResultTime: String, successful: Boolean, taskEndReason: TaskEndReasons)
//
//case class StageMetrics(jobId: Int, stageId: Int, name: String,
//                        submissionTime: String, completionTime: String, stageDuration: Long, numTasks: Int, executorRunTime: Long, executorCpuTime: Long,
//                        jvmGCTime: Long, peakExecutionMemory: Long, bytesRead:Long, bytesWritten:Long,  recordsRead:Long, recordsWritten:Long,
//                        taskList: ListBuffer[TaskMetrics])
//
//case class JobMetrics(jobId: Int, jobStartTime: String, jobEndTime: String, jobDuration: Long, jobResultstatus: String, stageList: ListBuffer[StageMetrics])
//
//case class JobVals(jobId: Int, taskList: ListBuffer[TaskMetrics])
//
//case class TaskEndReasons(className: String, description: String)
//
//
//case class AppVals(appId: String, appName: String, groups: String, subGroups: String, jobType: String, appStartTime: String, appEndTime: String,
//                   appDuration: Long, cpuTime: Long, runTime: Long, memoryUsage: Long, appStatus: String, appResult: String, failureREason: String,
//                   recordCount: String,numexecutors:String, numexecutorscores:String, executormemory:String, queueName:String, sparkUserName:String,
//                   numberOfTasks:Int, numberOfStages:Int, numberOfJobs:Int,
//                   bytesRead:Long, bytesWritten:Long,  recordsRead:Long, recordsWritten:Long)
//
//case class AppInfo(appId: String, appName: String, appStartTime: Long)
//
//
//
//
////  ************************ Custom Listener Starts from here ************************
//
//class ApplicationLevelMetricsSparkListener(
//                                            dualWriteFlag: String,
//                                            configOutputFileLocation: String,
//                                            kafkaPropertiesFileLocation: String
//                                          )(implicit spark: SparkSession, logger: Logger, properties: Properties) extends SparkListener {
//
//  val taskMetricsData: ListBuffer[TaskMetrics] = ListBuffer.empty[TaskMetrics]
//  val stageMetricsData: ListBuffer[StageMetrics] = ListBuffer.empty[StageMetrics]
//  val jobResultValData: ListBuffer[JobMetrics] = ListBuffer.empty[JobMetrics]
//
//  val appStartTime = System.currentTimeMillis()
//  val AppsData: ListBuffer[AppInfo] = ListBuffer.empty[AppInfo] //+= AppInfo(conf.getAppId, conf.get("spark.app.name"), System.currentTimeMillis())
//
//  val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
//
//  val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]
//  val StageIdtoStageNameJobId: collection.mutable.HashMap[Int, String] = collection.mutable.HashMap.empty[Int, String]
//
//  val JobtoTime: collection.mutable.HashMap[Int, Long] = collection.mutable.HashMap.empty[Int, Long]
//
//  val conf = spark.sparkContext.getConf
//  val isDebugLog = conf.get("spark.debug.listener", null)
//
//
//  val numexecutors = conf.get("spark.executor.instances","1")
//  val executorcores = conf.get("spark.executor.cores","1")
//  val executormemory = conf.get("spark.executor.memory","1")
//  val queueName = conf.get("spark.yarn.queue","default_queue")
//  val sparkUserName = spark.sparkContext.sparkUser
//
//  var numberOfTasks = 0
//  var numberOfStages = 0
//  var numberOfJobs = 0
//
//  var cpuTime = 0L
//  var runTime = 0L
//  var jvmGCTime = 0L
//  var memoryUsage = 0L
//
//  var inputRecords = 0L
//  var outputRecords = 0L
//
//  var inputBytes = 0L
//  var outputBytes = 0L
//
//
//  override def onApplicationStart(event: SparkListenerApplicationStart) {
//
//    val appInfo = AppInfo(event.appId.getOrElse(null), event.appName, event.time)
//
//    AppsData += appInfo
//
//  }
//
//
//  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
//
//    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
//    JobtoTime += (jobStart.jobId -> jobStart.time)
//
//    for (stageVals <- stageMetricsData) {
//      cpuTime += stageVals.executorCpuTime
//      runTime += stageVals.executorRunTime
//      jvmGCTime += stageVals.jvmGCTime
//      memoryUsage += stageVals.peakExecutionMemory
//    }
//
//    if (memoryUsage > 0 && memoryUsage >= 1024)  { memoryUsage = memoryUsage / 1024 }
//    if (cpuTime > 0 && runTime >= 1000) { cpuTime = cpuTime / 1000 }
//    if (runTime > 0 && runTime >= 1000) { runTime = runTime / 1000 }
//
//  }
//
//
//
//  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
//
//    val taskInfo = taskEnd.taskInfo
//
//    val gettingResultTime = {
//      if (taskInfo.gettingResultTime == 0L) "0"
//      else new SimpleDateFormat(dateFormat).format(new Date(taskInfo.finishTime - taskInfo.gettingResultTime))
//    }
//    val duration = (taskInfo.finishTime - taskInfo.launchTime) / 1000
//    val jobId = StageIdtoJobId(taskEnd.stageId)
//
//    val currentTask = TaskMetrics(jobId, taskEnd.stageId, taskInfo.taskId, new SimpleDateFormat(dateFormat).format(new Date(taskInfo.launchTime)),
//      new SimpleDateFormat(dateFormat).format(new Date(taskInfo.finishTime)), duration,
//
//      taskInfo.executorId, taskInfo.host, encodeTaskLocality(taskInfo.taskLocality),
//      taskInfo.speculative, gettingResultTime, taskInfo.successful, taskEndReasonToJson(taskEnd.reason))
//    taskMetricsData += currentTask
//
//  }
//
//  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
//    taskLocality match {
//      case TaskLocality.PROCESS_LOCAL => 0
//      case TaskLocality.NODE_LOCAL => 1
//      case TaskLocality.RACK_LOCAL => 2
//      case TaskLocality.NO_PREF => 3
//      case TaskLocality.ANY => 4
//    }
//  }
//
//  def taskEndReasonToJson(taskEndReason: TaskEndReason): TaskEndReasons = {
//
//    var taskEndReasons: TaskEndReasons = null
//    var className: String = null
//    var description: String = null
//    taskEndReason match {
//      case Success => taskEndReasons = TaskEndReasons(className, "Success")
//      case x: TaskKilled => taskEndReasons = TaskEndReasons(className, "Task Killed")
//      case exceptionFailure: ExceptionFailure => {
//
//        className = exceptionFailure.className
//        description = exceptionFailure.description
//        taskEndReasons = TaskEndReasons(className, description)
//
//      }
//    }
//
//    taskEndReasons
//  }
//
//  def getTaskForStage(stageId: Int): ListBuffer[TaskMetrics] = {
//
//    val taskData: ListBuffer[TaskMetrics] = taskMetricsData.filter(_.stageId == stageId)
//    taskData
//  }
//
//  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
//
//    val stageInfo = stageCompleted.stageInfo
//    val taskMetrics = stageInfo.taskMetrics
//
//    val jobId = StageIdtoJobId(stageInfo.stageId)
//    val taskData = getTaskForStage(stageCompleted.stageInfo.stageId)
//
//    val bytesRead = taskMetrics.inputMetrics.bytesRead
//    val recordsRead = taskMetrics.inputMetrics.recordsRead
//    val bytesWritten = taskMetrics.outputMetrics.bytesWritten
//    val recordsWritten = taskMetrics.outputMetrics.recordsWritten
//
//    println(s"bytesRead:$bytesRead \t recordsRead:$recordsRead \t bytesWritten:$bytesWritten \t  recordsWritten:$recordsWritten ")
//
//    val currentStage = StageMetrics(jobId, stageInfo.stageId, stageInfo.name,
//      new SimpleDateFormat(dateFormat).format(stageInfo.submissionTime.getOrElse(0L)), new SimpleDateFormat(dateFormat).format(stageInfo.completionTime.getOrElse(0L)),
//      (stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L)) / 1000,
//      stageInfo.numTasks, taskMetrics.executorRunTime, taskMetrics.executorCpuTime / 1000000, taskMetrics.jvmGCTime, taskMetrics.peakExecutionMemory,
//      bytesRead , bytesWritten ,  recordsRead , recordsWritten, taskData)
//    stageMetricsData += currentStage
//
//
//    // if (!stageInfo.name.contains("count") && !stageInfo.name.contains("collect") && !stageInfo.name.contains("take") && !stageInfo.name.contains("show")) {
//
//    if (stageInfo.name.contains("csv") || stageInfo.name.contains("orc") || stageInfo.name.contains("parquet") ||
//      stageInfo.name.contains("save")|| stageInfo.name.contains("insert")  ) {
//
//      println(s"stage Name: ${stageInfo.name}")
//
//      if (taskMetrics.inputMetrics != None) {
//        inputRecords += taskMetrics.inputMetrics.recordsRead
//        inputBytes += taskMetrics.inputMetrics.bytesRead
//      }
//      if (taskMetrics.outputMetrics != None) {
//        outputRecords += taskMetrics.outputMetrics.recordsWritten
//        outputBytes += taskMetrics.outputMetrics.bytesWritten
//      }
//    }
//
//  }
//
//  def getStageForJob(jobId: Int): ListBuffer[StageMetrics] = {
//    val stageData: ListBuffer[StageMetrics] = stageMetricsData.filter(_.jobId == jobId)
//    stageData
//  }
//
//  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
//
//    val jobId = jobEnd.jobId
//    val stageData = getStageForJob(jobEnd.jobId)
//    val jobDuration = (jobEnd.time - JobtoTime(jobEnd.jobId)) / 1000
//
//    val jobResultVal = JobMetrics(jobId, new SimpleDateFormat(dateFormat).format(JobtoTime(jobEnd.jobId)),
//      new SimpleDateFormat(dateFormat).format(new Date(jobEnd.time)), jobDuration, jobEnd.jobResult.toString(), stageData)
//
//    jobResultValData += jobResultVal
//
//    for (stageVals <- stageMetricsData) {
//      cpuTime += stageVals.executorCpuTime
//      runTime += stageVals.executorRunTime
//      jvmGCTime += stageVals.jvmGCTime
//      memoryUsage += stageVals.peakExecutionMemory
//    }
//
//    if (memoryUsage > 0 && memoryUsage >= 1024)  { memoryUsage = memoryUsage / 1024 }
//    if (cpuTime > 0 && runTime >= 1000) { cpuTime = cpuTime / 1000 }
//    if (runTime > 0 && runTime >= 1000) { runTime = runTime / 1000 }
//
//    numberOfTasks = taskMetricsData.map(_.taskId).toList.distinct.size
//    numberOfStages = stageMetricsData.map(_.stageId).toList.distinct.size
//    numberOfJobs = stageMetricsData.map(_.jobId).toList.distinct.size
//
//  }
//
//
//  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
//
//    logger.info("Application Completed and trying to write the Metrics")
//    logger.info(s"Output file location - , $configOutputFileLocation")
//
//    val jsonObj = new JSONObject()
//
//    val configs = spark.sparkContext.getConf.getAll
//    configs.foreach(config => jsonObj.put(config._1, config._2))
//    jsonObj.put("spark.user", spark.sparkContext.sparkUser)
//
//    val appInfo = AppInfo(conf.getAppId, conf.get("spark.app.name","default_app_name"), appStartTime)
//    val appEndDateTime = new Date(applicationEnd.time);
//    val sampleDf = new SimpleDateFormat(dateFormat);
//    val appDuration = (applicationEnd.time - appInfo.appStartTime) / 1000
//
//    for (stageVals <- stageMetricsData) {
//      cpuTime += stageVals.executorCpuTime
//      runTime += stageVals.executorRunTime
//      jvmGCTime += stageVals.jvmGCTime
//      memoryUsage += stageVals.peakExecutionMemory
//    }
//
//    if (null != isDebugLog && isDebugLog == "Y") {
//      println("MemUsage == " + memoryUsage)
//      println("cpuTime == " + cpuTime)
//      println("runTime == " + runTime)
//    }
//
//    if (memoryUsage > 0 && memoryUsage >= 1024)  { memoryUsage = memoryUsage / 1024 }
//    if (cpuTime > 0 && runTime >= 1000) { cpuTime = cpuTime / 1000 }
//    if (runTime > 0 && runTime >= 1000) { runTime = runTime / 1000 }
//
//    numberOfTasks = taskMetricsData.map(_.taskId).toList.distinct.size
//    numberOfStages = stageMetricsData.map(_.stageId).toList.distinct.size
//    numberOfJobs = stageMetricsData.map(_.jobId).toList.distinct.size
//
//
//    jsonObj.put("appId", appInfo.appId)
//    jsonObj.put("appName", appInfo.appName)
//    jsonObj.put("groups", conf.get("spark.group", ""))
//    jsonObj.put("subGroups", conf.get("spark.subGroup", ""))
//    jsonObj.put("jobType", conf.get("spark.jobtype", ""))
//    jsonObj.put("appStartTime", new SimpleDateFormat(dateFormat).format(appInfo.appStartTime))
//    jsonObj.put("appEndTime", sampleDf.format(appEndDateTime))
//    jsonObj.put("appDuration", appDuration)
//    jsonObj.put("cpuTime", cpuTime)
//    jsonObj.put("runTime", runTime)
//    jsonObj.put("memoryUsage", memoryUsage)
//    jsonObj.put("appStatus", "Complete")
//    jsonObj.put("appResult", "Success")
//    jsonObj.put("failureREason", getJobError())
//    jsonObj.put("numexecutors", numexecutors)
//    jsonObj.put("executorcores", executorcores)
//    jsonObj.put("executormemory", executormemory)
//    jsonObj.put("queueName", queueName)
//    jsonObj.put("sparkUserName", sparkUserName)
//    jsonObj.put("numberOfTasks", numberOfTasks)
//    jsonObj.put("numberOfStages", numberOfStages)
//    jsonObj.put("numberOfJobs", numberOfJobs)
//    jsonObj.put("inputBytes", inputBytes)
//    jsonObj.put("outputBytes", outputBytes)
//    jsonObj.put("inputRecords", inputRecords)
//    jsonObj.put("outputRecords", outputRecords)
//
//    println(s"Spark Job Metrics: \n ${jsonObj.toString()}")
//
//    //val keys =
//      jsonObj.keys.foreach(key => println(s"$key: ${jsonObj.get(key.toString)}"))
//
////    while ( jsonObj.keys.hasNext){
////    try {
////  val key = keys.next().toString
////  println(s"$key: ${jsonObj.get(key)}")
////          }catch {
////      case e:Exception=> println("Excetion occured...")
////    }
////    }
//
////    dualWriteFlag match {
////      case "HDFS" =>
////        logger.info("Metrics output flag is selected as HDFS")
////        produceJsonToHDFS(jsonObj.toString())
////      case "KAFKA" =>
////        logger.info("Metrics output flag is selected as KAFKA")
////        produceJsonToKafka(jsonObj, "spark.metrics.test")
////      case "DEFAULT_WRITE_FLAG" | _ => logger.info("Metrics output flag is selected as DEFAULT or BOTH HDFS & KAFKA")
////        if(produceJsonToKafka(jsonObj, "spark.metrics.test")) {
////          produceJsonToHDFS(jsonObj.toString())
////        }
////    }
//
//    logger.info("Metrics written successfully to the output location")
//
//  }
//
//  def produceJsonToHDFS(jsonString: String): Unit = {
//    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
//    hadoopConfiguration.setBoolean("dfs.support.append", true)
//
//    val fs = FileSystem.get(hadoopConfiguration)
//    val filePath = new Path(configOutputFileLocation)
//    val fileOutputStream = if(fs.exists(filePath)) {
//      logger.warn("File already exists, trying to append metrics")
//      fs.append(filePath)
//    } else {
//      logger.info("New metrics file created")
//      fs.create(filePath)
//    }
//
//    logger.info("Writing the metrics to the file")
//    val printWriter = new PrintWriter(fileOutputStream)
//    logger.info(s"Metrics JSON - $jsonString")
//    printWriter.write(jsonString + "\n")
//
//    printWriter.flush()
//    fileOutputStream.hflush()
//
//    printWriter.close()
//    fileOutputStream.close()
//  }
//
//  def produceJsonToKafka(jsonString: Object, kafkaTopic: String): Boolean = {
//    val producer = new KafkaProducer[String, Object](properties)
//    val producerRecord = new ProducerRecord[String, Object](kafkaTopic, null, jsonString)
//    val recordMetadata = producer.send(producerRecord).get()
//
//    recordMetadata.hasOffset
//  }
//
//  def getJobError(): String = {
//
//    var result: String = ""
//    val tasData: ListBuffer[JobMetrics] = jobResultValData.filter(_.jobResultstatus != "JobSucceeded")
//    if (tasData.nonEmpty) {
//      for (data <- tasData) {
//        result = data.jobResultstatus
//        if (result != "JobSucceeded") {
//          return result
//        }
//      }
//    } else {
//
//      result = "" // getLogError
//    }
//
//    result
//  }
//
//}