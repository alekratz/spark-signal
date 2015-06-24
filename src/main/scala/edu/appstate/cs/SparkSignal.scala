package edu.appstate.cs

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import uk.co.labbookpages.WavFile

object SparkSignal {

  def loadWavHDFS(sc: SparkContext, path: String): RDD[Double] = {
    val wavRdd = sc.binaryFiles(path)
    // get the stream
    val stream = wavRdd
      .first()
      ._2
      .open()
    // Load it into memory
    val wavFile = WavFile.openWavStream(stream)
    val frameCount = wavFile.getNumFrames.toInt
    println("loading wav with frame count of " + frameCount)
    val buffer = Array.ofDim[Double](frameCount)
    wavFile.readFrames(buffer, frameCount)
    sc.parallelize(buffer)
  }

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("usage: SparkSignal file1.wav [ file2.wav ... ]")
      System.exit(1)
    }
    val appName = "ScalaSignal"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    // run through all of the paths specified on the command line
    for(path <- args) {
      val wavBuffer = loadWavHDFS(sc, path)
      println("loaded an RDD of wav file " + path + " and size " + wavBuffer.count())
    }
  }
}
