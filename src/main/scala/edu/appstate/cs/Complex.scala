package edu.appstate.cs

case class Complex(re: Double, im: Double) {
  def +(x: Complex): Complex = Complex(re + x.re, im + x.im)
  def -(x: Complex): Complex = Complex(re - x.re, im - x.im)
  def *(x: Double): Complex = Complex(re * x, im * x)
  def *(x: Complex): Complex = Complex(re * x.re - im * x.im, re * x.im + im * x.re)
  def /(x: Double):  Complex = Complex(re / x, im / x)

  override def toString: String = {
    val a = "%1.3f" format re
    val b = "%1.3f" format im
    (a, b) match {
      case (_, "0.000") => a
      case ("0.000", _) => b + "i"
      case (_, _) if im > 0 => a + " + " + b + "i"
      case (_, _) => a + " - " + b + "i"
    }
  }
}

object Complex {
  def parse(str: String): Complex = {
    var re: Double = 0
    var im: Double = 0
    str.replace(" ", "")
      .split('+')
      .foreach(p => if(p.endsWith("i")) im += p.replace("i", "").toDouble
                    else re += p.toDouble)
    Complex(re, im)
  }
}