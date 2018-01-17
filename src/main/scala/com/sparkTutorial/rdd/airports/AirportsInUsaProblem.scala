package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils.COMMA_DELIMITER
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.textFile("in/airports.text")
      .map(_.split(COMMA_DELIMITER))
      .filter(_ (3) == "\"United States\"")
      .map(ss => Array(ss(1), ss(2)).mkString(","))
      .saveAsTextFile("out/airports_in_usa.text")

  }
}
