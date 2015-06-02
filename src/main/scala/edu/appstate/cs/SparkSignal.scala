package edu.appstate.cs

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import uk.co.labbookpages.WavFile

object SparkSignal {
  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      System.err.println("usage: SparkSignal filename.wav")
      System.exit(1)
    }
    val path = args(0)
    val appName = "ScalaSignal"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    // this returns an RDD with a (String, PortableDataStream)
    val wavRdd = sc.binaryFiles(path)
    println("-------------------------")
    for((filename, stream) <- wavRdd) {
      println("loaded file " + filename)
      println("with stream length of " + stream.open().available())
      stream.close()
    }
    println("-------------------------")

    /*
    val counts = contents.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(path + ".results")
    */
  }
}
