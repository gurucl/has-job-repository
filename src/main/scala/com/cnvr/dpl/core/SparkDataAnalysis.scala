package com.cnvr.dpl.core

import com.cnvr.dpl.listener.{ApplicationLevelMetricsSparkListener, MetricsListener, MyListener}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

  object SparkDataAnalysis {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark = SparkSession.builder()
      .appName("Spark_data_analysis")
      .master("local[1]")
      //.config("spark.extraListeners",new MetricsListener())
      .getOrCreate()

    implicit val sc = spark.sparkContext

    sc.setLogLevel("Error")

  //  sc.addSparkListener(new MyListener(sc))

    sc.addSparkListener(new MetricsListener())

   // sc.addSparkListener(new ApplicationLevelMetricsSparkListener())



    val columns = "eid,fname,lname,sal,dept".split(",") //.map(col(_))

    val df1 = spark.read.option("inferSchema","true")
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .load("file:///Users/gurcl/IdeaProjects/MyProject/src/main/resources/emp.csv")

      val df = df1.toDF(columns:_*)

    df.printSchema()

    df.show(false)

    val counter = sc.textFile("file:///Users/gurcl/IdeaProjects/MyProject/src/main/resources/emp.csv").count()

    println(s"count is :$counter")

    // df.na.drop().show(false) // Both are same =>  df.na.drop("any").show(false)

    // df.na.drop("all").show(false)

    // df.na.drop(Array("eid")).show(false)

    // df.na.fill(99).show(false)

   // df.na.fill("NA").show(false)

    val df2 = df.na.fill(Map("sal"->1000,"dept"->"NA"))//.show(false)

    df2.groupBy("dept").agg(count("*").as("Members Count"),
      sum("sal").as("tot_sal"), avg("sal").as("avg_sal") ).show(false)

    import spark.implicits._

    val df3 = df2.withColumn("dept_rank", rank().over(Window.partitionBy("dept").orderBy($"sal".desc)))
      .withColumn("dept_dense_rank", dense_rank().over(Window.partitionBy("dept").orderBy($"sal".desc)))
      .withColumn("row_number_sal", row_number().over(Window.orderBy(col("sal").desc)))

    df3.show(false)


    df3.filter($"dept_rank"===2).show(false)

    df.write.mode(SaveMode.Append).csv("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_csv_output.txt")

//    println("==============================================================")
//
//    df.write.mode(SaveMode.Append).orc("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_orc_output.txt")
//
//    println("==============================================================")
//
//    df.write.mode(SaveMode.Append).parquet("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_parquet_output.txt")
//
//    println("==============================================================")
//
//    df.write.mode(SaveMode.Append).save("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_save_output.txt")
//
//    println("==============================================================")
//
//    df.write.mode(SaveMode.Append).json("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_json_output.txt")
//
//    //df.write.mode(SaveMode.Append).("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test_text_output.txt")
//
//   // df.write.mode(SaveMode.Append).
//

    println(s"Spark Job has been completed successfully in ${(System.currentTimeMillis()-startTime)/1000} Seconds")

    println(s"DF has ${df.rdd.getNumPartitions} partitions...")
  }

}
