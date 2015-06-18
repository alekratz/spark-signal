package edu.appstate.cs

import java.io.File

import com.mpatric.mp3agic.Mp3File
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
    val buffer = Array.ofDim[Double](frameCount)
    wavFile.readFrames(buffer, frameCount)
    sc.parallelize(buffer)
  }

  def main(args: Array[String]): Unit = {
    if(args.length == 0) {
      System.err.println("usage: SparkSignal filename")
      System.exit(1)
    }
    println("opening " + args.length + " files")

    val appName = "ScalaSignal"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    // run through all of the paths specified on the command line
    for(path <- args) {
      println("opening " + path)

      path.split("\\.").last match {
        case "mp3" => {
          val mp3 = new Mp3File(sc, path);
          println("loaded an MP3 file " + path + " that is " + mp3.getLengthInMilliseconds + " milliseconds long")
        }
        case "wav" => {
          val wavBuffer = loadWavHDFS(sc, path)
          println("loaded an RDD of wav file " + path + " and size " + wavBuffer.count())
        }
      }
    }
  }
}
