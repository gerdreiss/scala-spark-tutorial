package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkConf, SparkContext}

object UnionLogProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
       take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */

    val conf = new SparkConf().setAppName("nasaLogs").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val june = sc.textFile("in/nasa_19950701.tsv")
    val august = sc.textFile("in/nasa_19950801.tsv")

    val notHeader: String => Boolean =
      line => !(line.startsWith("host") && line.endsWith("bytes"))

    (june union august)
      .sample(withReplacement = true, fraction = 0.1)
      .filter(notHeader)
      .saveAsTextFile("out/sample_nasa_logs.tsv")

  }
}
