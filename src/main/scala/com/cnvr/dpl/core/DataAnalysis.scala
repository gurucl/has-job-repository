package com.cnvr.dpl.core

import scala.io.Source

object DataAnalysis {

  def main(args: Array[String]): Unit = {

    val bs = Source.fromFile("src/main/resources/emp.csv")

    val lines = bs.getLines().toList

    val salesRecords = lines.map( line=>{

      val record = getSalesRecords(line)

      if (record.isRight) (true, record.right.get)

      else (false, line)

    })

    val goodRecords = salesRecords.filter(_._1==true).map(_._2).map(_.asInstanceOf[SalesRecord])

    val badRecords = salesRecords.filter(_._1==false).map(_._2)

    println("========== Good Records ==========")

    goodRecords.foreach(println)

    println("\n========== Bad Records ==========")

    badRecords.foreach(println(_))

    println("\n========== Total Salary by Depts ==========")

    goodRecords.map(x=>(x.dept, x.sal)).groupBy( x=> x._1).map(x=>(x._1, x._2.map(_._2).sum))

      .foreach(println)

    println("\n========== Average Salary by Depts ==========")

    goodRecords.map(x=>(x.dept, x.sal)).groupBy( x=> x._1).map(x=>(x._1, x._2.map(_._2).sum/x._2.size))

      .foreach(println)

    println("\n========== All details about Depts ==========")

    goodRecords.map(x=>(x.dept, x.sal)).groupBy( x=> x._1).map(x=>(x._1, x._2.size, x._2.map(_._2).sum, x._2.map(_._2).sum/x._2.size))

      .map{case(a,b,c,d)=> println(s"Dept $a has: $b Members and Total Salary:$c and Average Salary: $d")}


    bs.close()
  }

  def getSalesRecords(line:String):Either[MalFormedException,SalesRecord] = {

    val fields = line.split(",")

    if (fields.length!=5) Left(new MalFormedException)
    else Right(SalesRecord(fields(0).toInt, fields(1) ,fields(2) ,fields(3).toDouble, fields(4)))

  }

}

case class SalesRecord(eid:Int, fname:String, lname:String, sal:Double, dept:String)

class MalFormedException extends Exception {}
