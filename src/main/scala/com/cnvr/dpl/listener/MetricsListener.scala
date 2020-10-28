package com.cnvr.dpl.listener

import java.io.{FileOutputStream, PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.cnvr.dpl.core.KafkaProducer

import scala.collection.mutable.ListBuffer
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.spark._
import org.apache.spark.scheduler._

case class StageVals(jobId: Int, stageId: Int, name: String,
                     submissionTime: String, completionTime: String, stageDuration: Long, numTasks: Int, executorRunTime: Long, executorCpuTime: Long,
                     jvmGCTime: Long, peakExecutionMemory: Long, bytesRead:Long, bytesWritten:Long,  recordsRead:Long, recordsWritten:Long,
                     taskList: ListBuffer[TaskVals])

case class JobVals(jobId: Int, taskList: ListBuffer[TaskVals])

case class JobResultVal(jobId: Int, jobStartTime: String, jobEndTime: String, jobDuration: Long, jobResultstatus: String, stageList: ListBuffer[StageVals])
case class TaskEndReasons(
                           className: String,
                           description: String)

case class TaskVals(jobId: Int, stageId: Int, taskId: Long, launchTime: String, finishTime: String,
                    duration: Long, executorId: String, host: String, taskLocality: Int,
                    speculative: Boolean, gettingResultTime: String, successful: Boolean, taskEndReason: TaskEndReasons)

case class AppVals(appId: String, appName: String, groups: String, subGroups: String, jobType: String, appStartTime: String, appEndTime: String,
                   appDuration: Long, cpuTime: Long, runTime: Long, memoryUsage: Long, appStatus: String, appResult: String, failureREason: String,
                   recordCount: String,numexecutors:String, numexecutorscores:String, executormemory:String, queueName:String, sparkUserName:String,
                   numberOfTasks:Int, numberOfStages:Int, numberOfJobs:Int,
                    bytesRead:Long, bytesWritten:Long,  recordsRead:Long, recordsWritten:Long)

// case class LogVals(jobId: String, batchType: String, batchDate: String,
 //                  job: String, jobGroup: String, jobSubGroup: String, curDateTime: String, event: String, failureReason: String, totalRecordCount: Long)

case class AppInfo(appId: String, appName: String, appStartTime: Long)

// case class LogInfo(batchId: String, batchType: String, batchDate: String, jobName: String, jobGroup: String, jobSubGroup: String, currDate: String, event: String, failureReason: String, recordCount: String)

class MetricsListener( sc: SparkContext) extends SparkListener {

  val conf = sc.getConf

  val stageMetricsData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]
  val taskMetricsData: ListBuffer[TaskVals] = ListBuffer.empty[TaskVals]

  val jobResultValData: ListBuffer[JobResultVal] = ListBuffer.empty[JobResultVal]

 // val LogsData: ListBuffer[LogVals] = ListBuffer.empty[LogVals]

  val AppsData: ListBuffer[AppInfo] = ListBuffer.empty[AppInfo] += AppInfo(conf.getAppId, conf.get("spark.app.name"), System.currentTimeMillis())

  val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  val StageIdtoJobId: collection.mutable.HashMap[Int, Int] = collection.mutable.HashMap.empty[Int, Int]
  val StageIdtoStageNameJobId: collection.mutable.HashMap[Int, String] = collection.mutable.HashMap.empty[Int, String]
 // val StageInfoData: ListBuffer[StageVals] = ListBuffer.empty[StageVals]

  val JobtoTime: collection.mutable.HashMap[Int, Long] = collection.mutable.HashMap.empty[Int, Long]

  //   def getEncrypterFromFile():String ={
  //
  //    val fileContents = Source.fromFile(conf.get(ELasticConnConstants.ES_ENCRYPTPASS_PATH)).getLines.mkString
  //
  //    return fileContents
  //  }
  //
  //  def executeCommand(): String = {
  //
  //    var sbld = new StringBuilder("java -cp ").append(conf.get(ELasticConnConstants.ES_PWDJAR_PATH)).append("  ")
  //      .append(conf.get(ELasticConnConstants.ES_PWDJAR_CLASS)).append("  ")
  //      .append(conf.get(ELasticConnConstants.ES_DECRPT_TYPE))
  //      .append("  ").append(conf.get(ELasticConnConstants.ES_ENCRYPT_PWD))
  //      .append("  ").append(getEncrypterFromFile())
  //      //dsddd
  //
  //    //     val sbs = "java -cp /mapr/datalake/optum/optuminsight/d_edh/dev/uhc/dlf_temp/dev/d_conf/password/PBEEncryption_Optum.jar com.optum.peds.dl.common.AesDesCrypt decrypt 1ebcvqxQAaTBu+VHs8xgvQ== `cat /mapr/datalake/optum/optuminsight/d_edh/dev/uhc/dlf_temp/dev/developer/user1/dlfesuser1.properties`"
  //    val pwd = Runtime.getRuntime.exec(sbld.toString()) //sbld.toString())
  //    val pwd1 = Runtime.getRuntime.exec(sbld.toString()) //sbld.toString())
  //   val source = scala.io.Source.fromInputStream(pwd.getInputStream).getLines().mkString
  //    return source.toString();
  //  }

  //val esController = new EsController(conf.get("spark.esconfig.path", null),executeCommand())
  //var restClient: RestHighLevelClient = ElasticConnect.getElasticClient(conf, "")


  val isDebugLog = conf.get("spark.debug.listener", null)

  var cpuTime = 0L
  var runTime = 0L
  var jvmGCTime = 0L
  var memoryUsage = 0L

//  var recordReadCount: Long = 0L
  var recordWriteCount: Long = 0L

  var inputRecords = 0l
  var outputRecords = 0l

  var inputBytes = 0l
  var outputBytes = 0l

  var outputWrittenshuff = 0l
  var outputWrittenshuffw = 0l
 // var outputCountWrittenshuff = 0l
 // var inputCountRecords = 0l
 // var outputCountWritten = 0l
 // var outputWrittenshuffCountw = 0l

  val numexecutors = conf.get("spark.executor.instances","1")
  val executorcores = conf.get("spark.executor.cores","1")
  val executormemory = conf.get("spark.executor.memory","1")
  val queueName = conf.get("spark.yarn.queue","default_queue")
  val sparkUserName = sc.sparkUser

  var numberOfTasks = 0
  var numberOfStages = 0
  var numberOfJobs = 0

  def sendScalaMail(subject: String, body: String, emailAlertDist: String): Unit = {
    import scala.sys.process._
    Seq("/bin/sh", "-c", "echo \"" + body + "\" | mailx -s \"" + subject + "\" -b " + emailAlertDist + " " + emailAlertDist).!
  }

  def sendJsonInsert(currentApp: AppVals) {

    try {
      implicit val formats = DefaultFormats
      val jsonString: String = write(currentApp)

      println(currentApp)
      //esController.insertRecordWithId("es_dlf_domain_feeds", "job_activity", currentApp.appId, jsonString)
      KafkaProducer.sendToKafkaWithNewProducer(currentApp)
      println(s"Method Name: sendJsonInsert \t message: ${currentApp.toString()}")
      //ElasticConnect.insertToElasticSearch(restClient, currentApp.appId, jsonString, "has_feeds", "_doc")
    } catch {
      case ex: Exception => {
        val sw = new StringWriter
        ex.printStackTrace(new PrintWriter(sw))
        throw new Exception(ex)
      }
    }

  }

  def sendJsonUpdate(currentApp: AppVals) {

    try {
      implicit val formats = DefaultFormats
      val jsonString: String = write(currentApp)
      // ElasticConnect.updateToElasticSearch(restClient, currentApp.appId, jsonString, "has_feeds", "_doc")
      println(currentApp)
     // println("Entered into the method: sendJsonUpdate")
       KafkaProducer.sendToKafkaWithNewProducer(currentApp)
     // println(s"Method Name: sendJsonUpdate \t message: ${currentApp.toString()}")
      // esController.updateRecordById("es_dlf_domain_feeds", "job_activity", currentApp.appId, jsonString)
    } catch {
      case ex: Exception => {
        //val sw = new StringWriter
        ex.printStackTrace()
        throw new Exception(ex)
      }
    }

  }

  override def onApplicationStart(event: SparkListenerApplicationStart) {

    println("onApplicationStart: called...")

    val appInfo = AppInfo(event.appId.getOrElse(null), event.appName, event.time)

    AppsData += appInfo

   // println(s"appInfo added to AppsData... ${AppsData.map(_.toString).mkString("$")}")
    val currentApp = AppVals(appInfo.appId, appInfo.appName, conf.get("spark.group", ""),
      conf.get("spark.subGroup", ""), conf.get("spark.jobtype", ""),
      new SimpleDateFormat(dateFormat).format(appInfo.appStartTime), new SimpleDateFormat(dateFormat).format(appInfo.appStartTime),
      0, 0, 0, 0, "Running", "", "", "0",numexecutors, executorcores, executormemory, queueName, sparkUserName, numberOfTasks, numberOfStages, numberOfJobs,
      inputBytes, outputBytes, inputRecords, outputRecords )



    try {
      sendJsonInsert(currentApp)
    }
    catch {
      case e: Exception => println("Error while making Insert in to Elastic DB");e.printStackTrace();e.printStackTrace()

    }

  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    // println("started extracting from AppsData...")

    val appInfo = AppsData.last

    jobStart.stageIds

    jobStart.stageIds.foreach(stageId => StageIdtoJobId += (stageId -> jobStart.jobId))
    JobtoTime += (jobStart.jobId -> jobStart.time)

    //StageInfoData
    var beginSnapshot = System.currentTimeMillis()
    val appDuration = (beginSnapshot - appInfo.appStartTime) / 1000

    val appResult = "Job Started"

    for (stageVals <- stageMetricsData) {
      cpuTime += stageVals.executorCpuTime
      runTime += stageVals.executorRunTime
      jvmGCTime += stageVals.jvmGCTime
      memoryUsage += stageVals.peakExecutionMemory
    }

    if (memoryUsage > 0 && memoryUsage >= 1024)
      memoryUsage = memoryUsage / 1024
    if (cpuTime > 0 && runTime >= 1000)
      cpuTime = cpuTime / 1000
    if (runTime > 0 && runTime >= 1000)
      runTime = runTime / 1000
    val currentApp = AppVals(appInfo.appId, appInfo.appName, conf.get("spark.group", ""), conf.get("spark.subGroup", ""), conf.get("spark.jobtype", ""),
      new SimpleDateFormat(dateFormat).format(appInfo.appStartTime), new SimpleDateFormat(dateFormat).format(appInfo.appStartTime),
      0, 0, 0, 0, "Running", "", "", "0",numexecutors,executorcores,executormemory,queueName,sparkUserName, numberOfTasks, numberOfStages, numberOfJobs, inputBytes, outputBytes, inputRecords, outputRecords )

    //  sendJsonUpdate(currentApp)

  }
  def getTaskForJobs(jobId: Int): ListBuffer[TaskVals] = {

    val tasData: ListBuffer[TaskVals] = taskMetricsData.filter(_.jobId == jobId)
    tasData
  }

  def getTaskForStage(stageId: Int): ListBuffer[TaskVals] = {

    val taskData: ListBuffer[TaskVals] = taskMetricsData.filter(_.stageId == stageId)
    taskData
  }
  def getStageForJob(jobId: Int): ListBuffer[StageVals] = {
    val stageData: ListBuffer[StageVals] = stageMetricsData.filter(_.jobId == jobId)
    stageData
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    val appInfo = AppsData.last
    val jobId = jobEnd.jobId
    val stageData = getStageForJob(jobEnd.jobId)
    val jobDuration = (jobEnd.time - JobtoTime(jobEnd.jobId)) / 1000
    val jobResultVal = JobResultVal(jobEnd.jobId, new SimpleDateFormat(dateFormat).format(JobtoTime(jobEnd.jobId)), new SimpleDateFormat(dateFormat).format(new Date(jobEnd.time)), jobDuration, jobEnd.jobResult.toString(), stageData)
    jobResultValData += jobResultVal

    for (stageVals <- stageMetricsData) {
      cpuTime += stageVals.executorCpuTime
      runTime += stageVals.executorRunTime
      jvmGCTime += stageVals.jvmGCTime
      memoryUsage += stageVals.peakExecutionMemory
    }

    var beginSnapshot = System.currentTimeMillis()
    val appDuration = (beginSnapshot - appInfo.appStartTime) / 1000

    if (memoryUsage > 0 && memoryUsage >= 1024)
      memoryUsage = memoryUsage / 1024
    if (cpuTime > 0 && runTime >= 1000)
      cpuTime = cpuTime / 1000
    if (runTime > 0 && runTime >= 1000)
      runTime = runTime / 1000

    numberOfTasks = taskMetricsData.map(_.taskId).toList.distinct.size
    numberOfStages = stageMetricsData.map(_.stageId).toList.distinct.size
    numberOfJobs = stageMetricsData.map(_.jobId).toList.distinct.size

    // val aggregatedExecutorRunTime = stageMetricsData.toList.filter(x => x.jobId == jobId).map(x => x.executorRunTime).sum
    val recordsReadByJob = stageMetricsData.toList.filter(x => x.jobId == jobId).map(x => x.recordsRead).sum
    val recordsWrittenByJob = stageMetricsData.toList.filter(x => x.jobId == jobId).map(x => x.recordsWritten).sum
    val bytesRead = stageMetricsData.toList.filter(x => x.jobId == jobId).map(x => x.bytesRead).sum
    val bytesWritten = stageMetricsData.toList.filter(x => x.jobId == jobId).map(x => x.bytesWritten).sum

    println(s"recordsReadByJob: $recordsReadByJob \t recordsWrittenByJob: $recordsWrittenByJob \t bytesRead: $bytesRead \t  bytesWritten:$bytesWritten ")

    val currentApp = AppVals(appInfo.appId, appInfo.appName, conf.get("spark.group", ""),
      conf.get("spark.subGroup", ""), conf.get("spark.jobtype", ""),
      new SimpleDateFormat(dateFormat).format(appInfo.appStartTime), new SimpleDateFormat(dateFormat).format(appInfo.appStartTime),
      appDuration, cpuTime, runTime, memoryUsage, "Running", "InProgress", "", "0", numexecutors, executorcores, executormemory, queueName, sparkUserName, numberOfTasks, numberOfStages, numberOfJobs, inputBytes, outputBytes, inputRecords, outputRecords )

    try {
      sendJsonUpdate(currentApp)
    }catch{
      case e:Exception=> println("Issue while making Json Update to Elastic")
    }

  }
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    val appInfo = AppsData.last

    val stageInfo = stageCompleted.stageInfo

    val taskMetrics = stageInfo.taskMetrics
    val jobId = StageIdtoJobId(stageInfo.stageId)
    val taskData = getTaskForStage(stageCompleted.stageInfo.stageId)

    val bytesRead = taskMetrics.inputMetrics.bytesRead
    val recordsRead = taskMetrics.inputMetrics.recordsRead
    val bytesWritten = taskMetrics.outputMetrics.bytesWritten
    val recordsWritten = taskMetrics.outputMetrics.recordsWritten

    println(s"bytesRead:$bytesRead \t recordsRead:$recordsRead \t bytesWritten:$bytesWritten \t  recordsWritten:$recordsWritten ")

    val currentStage = StageVals(jobId, stageInfo.stageId, stageInfo.name,
      new SimpleDateFormat(dateFormat).format(stageInfo.submissionTime.getOrElse(0L)), new SimpleDateFormat(dateFormat).format(stageInfo.completionTime.getOrElse(0L)),
      (stageInfo.completionTime.getOrElse(0L) - stageInfo.submissionTime.getOrElse(0L)) / 1000,
      stageInfo.numTasks, taskMetrics.executorRunTime, taskMetrics.executorCpuTime / 1000000, taskMetrics.jvmGCTime, taskMetrics.peakExecutionMemory,
      bytesRead , bytesWritten ,  recordsRead , recordsWritten, taskData)
    stageMetricsData += currentStage



   // if (!stageInfo.name.contains("count") && !stageInfo.name.contains("collect") && !stageInfo.name.contains("take") && !stageInfo.name.contains("show")) {

      if (stageInfo.name.contains("csv") || stageInfo.name.contains("orc") || stageInfo.name.contains("parquet") ||
        stageInfo.name.contains("save")|| stageInfo.name.contains("insert")  ) {

      println(s"stage Name: ${stageInfo.name}")

      if (taskMetrics.inputMetrics != None) {
        inputRecords += taskMetrics.inputMetrics.recordsRead
        inputBytes += taskMetrics.inputMetrics.bytesRead
      }
      if (taskMetrics.outputMetrics != None) {
        outputRecords += taskMetrics.outputMetrics.recordsWritten
        outputBytes += taskMetrics.outputMetrics.bytesWritten
      }

      outputWrittenshuff += taskMetrics.shuffleWriteMetrics.recordsWritten
      outputWrittenshuffw += taskMetrics.shuffleWriteMetrics.shuffleRecordsWritten
      outputWrittenshuff += outputRecords

    }

    /** Collect data from accumulators, with additional care to keep only numerical values */
    stageInfo.accumulables.foreach(acc => try {
      val value = acc._2.value.getOrElse(0L).asInstanceOf[Long]
      val name = acc._2.name.getOrElse("")
      if (null != isDebugLog && isDebugLog == "Y") {
        println("name is " + stageInfo.name)
        println("value is " + value)
      }

      if (stageInfo.name.contains("save") | stageInfo.name.contains("text") | stageInfo.name.contains("foreachPartition at DocumentRDDFunctions")) {
        if (name == "number of output rows") {

          recordWriteCount = value
          if (null != isDebugLog && isDebugLog == "Y") {
            println("value is " + recordWriteCount)
          }
        }

      }
      if (recordWriteCount == 0) {
        recordWriteCount = outputWrittenshuff
      }
    } catch {
      case ex: ClassCastException => None
    })

  }

  def encodeTaskLocality(taskLocality: TaskLocality.TaskLocality): Int = {
    taskLocality match {
      case TaskLocality.PROCESS_LOCAL => 0
      case TaskLocality.NODE_LOCAL => 1
      case TaskLocality.RACK_LOCAL => 2
      case TaskLocality.NO_PREF => 3
      case TaskLocality.ANY => 4
    }
  }
  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  def taskEndReasonToJson(taskEndReason: TaskEndReason): TaskEndReasons = {
   // val reason = getFormattedClassName(taskEndReason)
    var taskEndReasons: TaskEndReasons = null
    var className: String = null
    var description: String = null
    taskEndReason match {
      case Success => taskEndReasons = TaskEndReasons(className, "Success")
      case x: TaskKilled => taskEndReasons = TaskEndReasons(className, "Task Killed")
      case exceptionFailure: ExceptionFailure => {

        className = exceptionFailure.className
        description = exceptionFailure.description
        taskEndReasons = TaskEndReasons(className, description)

      }

    }

    taskEndReasons
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val appInfo = AppsData.last

    val taskInfo = taskEnd.taskInfo
  //  val taskMetrics = taskEnd.taskMetrics.inputMetrics.recordsRead

    val gettingResultTime = {
      if (taskInfo.gettingResultTime == 0L) "0"
      else new SimpleDateFormat(dateFormat).format(new Date(taskInfo.finishTime - taskInfo.gettingResultTime))
    }
    val duration = (taskInfo.finishTime - taskInfo.launchTime) / 1000
    val jobId = StageIdtoJobId(taskEnd.stageId)
    val currentTask = TaskVals(jobId, taskEnd.stageId, taskInfo.taskId, new SimpleDateFormat(dateFormat).format(new Date(taskInfo.launchTime)),
      new SimpleDateFormat(dateFormat).format(new Date(taskInfo.finishTime)), duration,

      taskInfo.executorId, taskInfo.host, encodeTaskLocality(taskInfo.taskLocality),
      taskInfo.speculative, gettingResultTime, taskInfo.successful, taskEndReasonToJson(taskEnd.reason))
    taskMetricsData += currentTask

  }

  def writeSerializedJSON(fullPath: String, metricsData: AnyRef): Unit = {
    val objectMapper = new ObjectMapper with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val objectWriter = objectMapper.writer(new DefaultPrettyPrinter())

    val os = new FileOutputStream(fullPath)
    try {
      objectWriter.writeValue(os, metricsData)
    } finally {
      os.close()
    }
  }
  //  def getLogError(): String = {
  //
  //    var status: String = ""
  //    val appInfo = AppsData.last
  //    if (null == conf.get("spark.hbase.log.table", "")) {
  //      return status
  //    }
  //    try {
  //      val hBaseConf = HBaseConfiguration.create()
  //      val connection = ConnectionFactory.createConnection(hBaseConf)
  //      val table = connection.getTable(TableName.valueOf(Bytes.toBytes(conf.get("spark.hbase.log.table"))))
  //
  //      if (null != table) {
  //        var get = new Get(Bytes.toBytes(appInfo.appName + "_" + appInfo.appId))
  //        get.addColumn(Bytes.toBytes("V"), Bytes.toBytes("exception"))
  //        val result = table.get(get)
  //        if (null != result) {
  //          val value = result.value()
  //          status = Bytes.toString(value)
  //        }
  //        table.close()
  //        hBaseConf.clear()
  //        connection.close()
  //      }
  //    } catch {
  //      case ex: Exception => {
  //        val sw = new StringWriter
  //        ex.printStackTrace(new PrintWriter(sw))
  //        println("Issue while inserting audit data into Hbase: " + ex.printStackTrace())
  //        ""
  //      }
  //    }
  //
  //    status
  //  }

  def getJobStatusApps(): Boolean = {

    var stat: Boolean = false
    val tasData: ListBuffer[JobResultVal] = jobResultValData.filter(_.jobResultstatus != "JobSucceeded")
    if (tasData.nonEmpty) {
      stat = false
    } else {
      var err = ""// getLogError
      if (null != err) {
        if (!err.isEmpty())
          stat = false
      } else {
        stat = true
      }
    }
    stat

  }
  def getJobError(): String = {

    var result: String = ""
    val tasData: ListBuffer[JobResultVal] = jobResultValData.filter(_.jobResultstatus != "JobSucceeded")
    if (tasData.nonEmpty) {
      for (data <- tasData) {
        result = data.jobResultstatus
        if (result != "JobSucceeded") {
          return result
        }
      }
    } else {

      result = "" //getLogError
    }

    result
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

    val appInfo = AppsData.last
    val appEndDateTime = new Date(applicationEnd.time);
    val sampleDf = new SimpleDateFormat(dateFormat);
    val appDuration = (applicationEnd.time - appInfo.appStartTime) / 1000

    val appResult = {
      if (getJobStatusApps == true) "Success"
      else "Failed"
    }

    for (stageVals <- stageMetricsData) {
      cpuTime += stageVals.executorCpuTime
      runTime += stageVals.executorRunTime
      jvmGCTime += stageVals.jvmGCTime
      memoryUsage += stageVals.peakExecutionMemory
    }

    if (null != isDebugLog && isDebugLog == "Y") {
      println("MemUsage == " + memoryUsage)
      println("cpuTime == " + cpuTime)
      println("runTime == " + runTime)
    }
    if (memoryUsage > 0 && memoryUsage >= 1024)
      memoryUsage = memoryUsage / 1024
    if (cpuTime > 0 && runTime >= 1000)
      cpuTime = cpuTime / 1000
    if (runTime > 0 && runTime >= 1000)
      runTime = runTime / 1000


    numberOfTasks = taskMetricsData.map(_.taskId).toList.distinct.size
    numberOfStages = stageMetricsData.map(_.stageId).toList.distinct.size
    numberOfJobs = stageMetricsData.map(_.jobId).toList.distinct.size

    val currentApp = AppVals(appInfo.appId, appInfo.appName, conf.get("spark.group", ""), conf.get("spark.subGroup", ""),
      conf.get("spark.jobtype", ""), new SimpleDateFormat(dateFormat).format(appInfo.appStartTime), sampleDf.format(appEndDateTime),
      appDuration, cpuTime, runTime, memoryUsage, "Complete", "Success", getJobError(), recordWriteCount.toString(),
      numexecutors,executorcores,executormemory,queueName,sparkUserName, numberOfTasks, numberOfStages, numberOfJobs,
      inputBytes, outputBytes, inputRecords, outputRecords)
    sendJsonUpdate(currentApp)

    // restClient.close()

  }

}
