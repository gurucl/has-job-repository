package com.cnvr.dpl.core

import org.apache.spark.sql.SparkSession


object SparkWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Word Count").master("local[1]").getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    val linesRDD = sc.textFile("file:///Users/gurcl/IdeaProjects/MyProject/src/main/resources/test.txt")

    linesRDD.flatMap(_.split("\\s+")).map(_.trim.toLowerCase().replaceAll("[-+,.:!&-]",""))

      .filter(_!="").map(x=>(x,1)).reduceByKey(_+_).sortBy(-_._2).take(10).foreach(println)





  }

}
