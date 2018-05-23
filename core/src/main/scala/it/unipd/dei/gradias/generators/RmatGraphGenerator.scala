package it.unipd.dei.gradias.generators

import it.unipd.dei.gradias.io.EdgeKey
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.util.Random

class RmatGraphGenerator(val scale: Int, val edgeFactor: Int) extends Generator {

  override def generate(): Iterator[(EdgeKey, Float)] = ???

  override def parallel(sc: SparkContext): RDD[(EdgeKey, Float)] = {
    val n: Long = math.pow(2, scale).toLong
    val m: Long = n*edgeFactor
    val numParts = sc.defaultParallelism
    val edgesPerPart = math.floor(m / numParts.toDouble).toLong
    val v = math.round(n.toDouble/2.0)

    sc.parallelize(0 until numParts).flatMap { _ =>
      0L until edgesPerPart map { _ =>
        var edge = new EdgeKey()

        edge = RmatGraphGenerator.chooseCell(v, v, v)
        if (edge.dst < edge.src)
          edge = new EdgeKey(edge.dst, edge.src)

        (edge, 1.0f)
      }
    }.reduceByKey((a,b) => a)
  }

}

object RmatGraphGenerator {

  val RMATa = 0.45
  val RMATb = 0.15
  val RMATc = 0.15
  val RMATd = 0.25

  @tailrec
  private def chooseCell(x: Long, y: Long, t: Long): EdgeKey = {
    if (t <= 1) {
      new EdgeKey(x, y)
    } else {
      val newT = math.round(t.toFloat/2.0).toInt
      pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
        case 0 => chooseCell(x, y, newT)
        case 1 => chooseCell(x + newT, y, newT)
        case 2 => chooseCell(x, y + newT, newT)
        case 3 => chooseCell(x + newT, y + newT, newT)
      }
    }
  }

  private def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters sum to " + (a + b + c + d)
        + ", should sum to 1.0")
    }
    val result = Random.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if x >= a && x < a + b => 1 // 1 corresponds to b
      case x if x >= a + b && x < a + b + c => 2 // 2 corresponds to c
      case _ => 3 // 3 corresponds to d
    }
  }

}