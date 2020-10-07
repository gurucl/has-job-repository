package com.cnvr.dpl.core

import scala.io.Source
import scala.math.Ordering.ordered

object WordCount {

  def main(args: Array[String]): Unit = {

    val bs = Source.fromFile("/Users/gurcl/IdeaProjects/MyProject/src/main/resources/test.txt")

    val lines = bs.getLines().toList

    val words = lines.flatMap(x => x.split("\\s+")).map(x => x.toLowerCase.replaceAll("[,.!-+&*?]",""))

      .filter(x => x!="").groupBy(x => x).map(x =>(x._1, x._2.size)).toList.sortBy(-_._2).take(10)

    words.foreach(println)

    bs.close()
  }

}
