package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, _}

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    sc.textFile("in/word_count.text")
      .flatMap(_.split(" "))
      .countByValue()
      .foreach {
        case (word, count) => println(s"$word:$count")
      }
  }
}
