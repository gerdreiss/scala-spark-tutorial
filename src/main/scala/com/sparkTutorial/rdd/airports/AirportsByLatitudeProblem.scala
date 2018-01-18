package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils.COMMA_DELIMITER
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.textFile("in/airports.text")
      .map(_.split(COMMA_DELIMITER))
      .filter(_(6).toFloat > 40)
      .map(ss => s"${ss(1)}, ${ss(6)}")
      .saveAsTextFile("out/airports_by_latitude.text")
  }
}
