package com.sparkTutorial.pairRdd.groupbykey

import com.sparkTutorial.commons.Utils.COMMA_DELIMITER
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByCountryProblem {


  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,
       output the the list of the names of the airports located in each country.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:

       "Canada", List("Bagotville", "Montreal", "Coronation", ...)
       "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
       "Papua New Guinea",  List("Goroka", "Madang", ...)
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("nasaLogs").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val cached: RDD[(String, String)] =
      sc.textFile("in/airports.text")
        .map(_.split(COMMA_DELIMITER))
        .map(ss => (ss(3).replace("\"", ""), ss(1).replace("\"", "")))
        .persist()

    // use group by key
    cached
      .groupByKey()
      .sortByKey()
      .foreach(t => println(f"${t._1}%32s: ${t._2.mkString(", ")}"))

    // use reduce by key, the preferred way
    cached
      .sortByKey()
      .reduceByKey((a, b) => s"$a, $b")
      .foreach(t => println(f"${t._1}%32s: ${t._2}"))
  }
}
