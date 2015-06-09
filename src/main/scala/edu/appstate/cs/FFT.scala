package edu.appstate.cs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.math.{Pi, cosh, sinh, cos, sin}

object FFT {
  def fft(cSeq: RDD[Complex], direction: Complex, scalar: Int): RDD[Complex] = {
    val n = cSeq.count().toInt
    if(n == 1) {
      return cSeq
    }
    assume(n % 2 == 0, "Cooley-Tukey FFT algorithm expects an even-length input of values")

    val (evenRef, oddRef) = getEvenOddPairs(cSeq)
    val evens = fft(evenRef, direction, scalar).collect()
    val odds = fft(oddRef, direction, scalar).collect()

    def leftRightPair(k: Int): (Complex, Complex) = {
      val base = evens(k) / scalar
      val offset = exp(direction * (Pi * k / n)) * odds(k) / scalar
      (base + offset, base - offset)
    }

    val pairs = (0 until n / 2) map leftRightPair
    val left = pairs.map(_._1)
    val right = pairs.map(_._2)
    cSeq.context.parallelize(left ++ right)
  }

  def  fft(cSeq: RDD[Complex]): RDD[Complex] = fft(cSeq, Complex(0, 2), 1)
  def ifft(cSeq: RDD[Complex]): RDD[Complex] = fft(cSeq, Complex(0, -2), 2)

  def main(args: Array[String]): Unit = {
    if(args.length == 0 || args.length % 2 != 0) {
      println("usage: FFT item1 item2 [ item3 item4 ... ]")
      println("FFT expects an even number of items in the sequence passed to it")
      System.exit(1)
    }
    val appName = "ScalaSignal"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    // construct a sequence
    val complexes = sc.parallelize(
      args.map(p => Complex.parse(p)))
    println("read complex values")
    complexes.collect()
      .foreach(cpx => println(cpx))
    val fourier = fft(complexes)
    val inverse = ifft(fourier)

    println("results of FFT")
    fourier.collect()
      .foreach(cpx => println(cpx))
    println("results of IFFT")
    inverse.collect()
      .foreach(cpx => println(cpx))
  }

  private def exp(c: Complex): Complex = {
    val r = cosh(c.re) + sinh(c.re)
    Complex(cos(c.im), sin(c.im)) * r
  }

  private def getEvenOddPairs(toGroup: RDD[Complex]): (RDD[Complex], RDD[Complex]) = {
    val evens = toGroup.zipWithIndex()
      .filter(tup => tup._2 % 2 == 0)
      .map(tup => tup._1)
    val odds = toGroup.zipWithIndex()
      .filter(tup => tup._2 % 2 == 1)
      .map(tup => tup._1)
    (evens, odds)
  }
}
