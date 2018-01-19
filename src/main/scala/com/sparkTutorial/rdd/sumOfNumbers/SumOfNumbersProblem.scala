package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("sum").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val result = sc.textFile("in/prime_nums.text")
      .flatMap(_.split("\\s"))
      .filter(!_.isEmpty)
      .map(_.toInt)
      .take(100)
      //.reduce(_ + _)
      .sum

    println(result)

  }
}
