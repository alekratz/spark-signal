package edu.appstate.cs

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

object SparkSignal {
  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      System.err.println("usage: SparkSignal filename")
      System.err.println("This will actually perform a wordcount for now to test out Spark development")
      System.exit(1)
    }
    val path = args(0)
    val appName = "ScalaSignal"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val contents = sc.textFile(path)



    val counts = contents.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(path + ".results")
  }
}
