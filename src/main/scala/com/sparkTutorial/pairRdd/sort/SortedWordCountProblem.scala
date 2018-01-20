package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem extends App {

  /*
     Create a Spark program to read the an article from in/word_count.text,
     output the number of occurrence of each word in descending order.

     Sample output:

     apple : 200
     shoes : 193
     bag : 176
     ...
   */

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
  val sc = new SparkContext(conf)

  sc.textFile("in/word_count.text")
    .flatMap(_.split("\\s+|\\.|,|\\(|\\)|\\[|\\]"))
    .filter(_.nonEmpty)
    .map((_, 1))
    .reduceByKey(_ + _)
    //.map(_.swap)
    //.sortByKey(ascending = false)
    //.map(_.swap)
    .sortBy(_._2, ascending = false)
    .collect
    .foreach {
      case (word, count) => println(s"$word : $count")
    }

}

