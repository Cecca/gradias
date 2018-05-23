/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.gradias

import java.io.{FileOutputStream, PrintStream}

import com.google.common.hash.{PrimitiveSink, Funnel}
import it.unimi.dsi.util.BloomFilter
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias.io.EdgeKey
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.util.Random

package object generators {

  def getGenerator(descr: String): Generator = {
    val generator = """(\w+)\((.*?)\)""".r
    descr match {
      case generator(name, params) =>
        name match {
          case "layered" =>
            params.split(",").toList match {
              case List(inputFile, layers) =>
                LayeredGraphGenerator.fromFile(inputFile.trim, layers.toInt, 1.0f)
              case List(inputFile, layers, interLayerWeight) =>
                LayeredGraphGenerator.fromFile(inputFile.trim, layers.toInt, interLayerWeight.toFloat)
              case err => throw new IllegalArgumentException(s"Wrong parameter list `$err`")
            }
          case "rmat" =>
            params.split(",").toList match {
              case List(scale, edgeFactor) => new RmatGraphGenerator(scale.trim.toInt, edgeFactor.trim.toInt)
              case List(scale) => new RmatGraphGenerator(scale.trim.toInt, 16)
              case err => throw new IllegalArgumentException(s"Wrong parameter list `$err`")
            }
          case "mesh" =>
            params match {
              case n => new Mesh(n.toLong)
            }
          case _ => throw new IllegalArgumentException(s"Unknown generator `$name`")
        }
      case _ => throw new IllegalArgumentException(descr)
    }
  }

  trait Generator {
    def generate(): Iterator[(EdgeKey, Float)]
    def parallel(sc: SparkContext): RDD[(EdgeKey, Float)]
  }

  object LinearArray {

    def apply(outFile: String, n: Int): Unit = {
      val out = new PrintStream(new FileOutputStream(outFile))
      apply(n).foreach { l =>
        out.println(l.mkString(" "))
      }
      out.close()
    }

    def apply(n: Int): Stream[Seq[Int]] = timed("Linear array generation") {
      @tailrec
      def generate(id: Int, s: Stream[Seq[Int]]): Stream[Seq[Int]] = {
        if (id == n)
          s
        else {
          val adj =
            id :: List(id+1, id-1).filter(valid(n))
          generate(id + 1, adj #:: s)
        }
      }
      generate(0, Stream.empty)
    }

    def valid(n: Int)(x: Int): Boolean = {
      (x < n) && (x >= 0)
    }

  }

  class Mesh(val n: Long) extends Generator {

    override def generate(): Iterator[(EdgeKey, Float)] = {
      val adjStream = Mesh(n.toInt)
      adjStream.flatMap { case src :: neighs =>
        neighs.flatMap { dst =>
          if (src < dst) Iterator( (new EdgeKey(src.toLong, dst.toLong), 1.0f) )
          else Iterator.empty
        }
      }.iterator
    }

    override def parallel(sc: SparkContext): RDD[(EdgeKey, Distance)] = ???
  }

  object Mesh {

    /**
     * Generates a n x n mesh
     */
    def apply(outFile: String, n: Int): Unit = {
      val out = new PrintStream(new FileOutputStream(outFile))
      apply(n).foreach { l =>
        out.println(l.mkString(" "))
      }
      out.close()
    }

    def apply(n: Int): Stream[Seq[Int]] = timed("Mesh generation") {
      @tailrec
      def generate(id: Int, s: Stream[Seq[Int]]): Stream[Seq[Int]] = {
        if (id == n*n)
          s
        else {
          val (row, col) = idToPair(n)(id)
          val adj =
            id ::
              List((row + 1, col), (row - 1, col), (row, col + 1), (row, col - 1)).filter(validPair(n)).map(pairToId(n))
          generate(id + 1, adj #:: s)
        }
      }
      generate(0, Stream.empty)
    }

    def valid(n: Int)(x: Int): Boolean = {
      (x < n) && (x >= 0)
    }

    def validPair(n:Int)(pair: (Int, Int)): Boolean = {
      valid(n)(pair._1) && valid(n)(pair._2)
    }

    def pairToId(n: Int)(pair: (Int, Int)): Int = {
      pair._1 + n*pair._2
    }

    def idToPair(n: Int)(id: Int): (Int, Int) = {
      (id % n, id / n)
    }

  }

}
