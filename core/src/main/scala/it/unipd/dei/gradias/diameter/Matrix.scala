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

package it.unipd.dei.gradias.diameter

import java.io.{FileInputStream, FileOutputStream, File}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.{Distance, Infinity}

import scala.util.Random

object Matrix {

  def write(matrix: Matrix, dir: String = "/tmp"): Option[File] = {
    val path = File.createTempFile("spark-matrix-", "", new File(dir))
    algoLogger.info("Writing adjacency matrix to {}", path)
    val kryo = new Kryo()
    val out = new Output(new FileOutputStream(path))
    try {
      kryo.writeObject(out, matrix.data)
      Some(path)
    } catch {
      case _: Exception => None
    } finally {
      out.close()
    }
  }

  def read(path: File): Matrix = {
    val kryo = new Kryo()
    val in = new Input(new FileInputStream(path))
    try {
      new Matrix(kryo.readObject(in, classOf[Array[Array[Distance]]]))
    } finally {
      in.close()
    }
  }

  def main(args: Array[String]) {
    val m = new Matrix(Array.fill[Distance](4,4)(Random.nextInt(10)))
    println("Random matrix:")
    println(m)
    val p = write(m)
    val rm = read(p.get)
    println("Loaded matrix:")
    println(rm)
  }

}

class Matrix(val data: Array[Array[Distance]]) {

  val dim = data.length

  def floydWarshall(): Matrix = timed("Matrix.floydWarshall")  {
    var k =0
    while(k<dim) {
      var i = 0
      while(i<dim) {
        var j= 0
        while(j<dim) {
          if(data(i)(k) != Infinity && data(k)(j) != Infinity) {
            val sum = data(i)(k) + data(k)(j)
            if(data(i)(j) > sum) {
              data(i)(j) = sum
            }
          }
          j += 1
        }
        i += 1
      }
      k += 1
    }
    this
  }

  def floydWarshallSymmetric(): Matrix = timed("Matrix.floydWarshall") {
    val dim = data.length
    var k =0
    while(k<dim) {
      var i = 0
      while(i<dim) {
        var j = i
        while(j<dim) {
          if(data(i)(k) != Infinity && data(k)(j) != Infinity) {
            val sum = data(i)(k) + data(k)(j)
            if(data(i)(j) > sum) {
              data(i)(j) = sum
              data(j)(i) = sum
            }
          }
          j += 1
        }
        i += 1
      }
      k += 1
    }
    this
  }

  def adjacencyOnly: Matrix = {
    new Matrix(data.clone().map { row =>
      row.map{
        case 0.0f => 0.0f
        case Infinity => Infinity
        case _ => 1.0f
      }
    })
  }

  def minPlusProd(other: Matrix): Matrix = timed("Matrix.minPlusProduct") {
    var i = 0
    val res = Array.ofDim[Distance](dim, dim)

    while(i < dim) {
      var j = 0
      while(j < dim) {
        var acc = Infinity
        var k = 0
        while (k < dim) {
          acc = math.min(acc, this.data(i)(k) + other.data(k)(j))
          k += 1
        }
        res(i)(j) = acc
        j += 1
      }
      i += 1
    }
    new Matrix(res.map(_.toArray))
  }

  def isSymmetric: Boolean = timed("Matrix.isSymmetric") {
    var i = 0
    while(i<dim) {
      var j = 0
      while(j<dim) {
        if(data(i)(j) != data(j)(i)) {
          return false
        }
        j += 1
      }
      i += 1
    }
    return true
  }

  def square: Matrix = this minPlusProd this

  def closure: Matrix = timed("Matrix.closure") {
    if(!this.hasInfiniteElement) {
      return this
    }
    var i = 0
    var prev = this
    var cur = prev.square
    while(cur.hasInfiniteElement && i < math.ceil(math.log(dim)/math.log(2)).toInt) {
      i += 1
      prev = cur
      cur = prev.square
    }
    cur
  }

  def hasInfiniteElement: Boolean = timed("Matrix.hasInfiniteElement") {
    var i = 0
    while(i<dim) {
      var j = 0
      while(j<dim) {
        if(data(i)(j) == Infinity) {
          return true
        }
        j += 1
      }
      i += 1
    }
    return false
  }

  def max: Distance = timed("Matrix.max") {
    data.map(_.max).max
  }

  def sum(other: Matrix): Matrix = {
    var i = 0
    while(i<dim) {
      var j = 0
      while(j<dim) {
        this.data(i)(j) += other.data(i)(j)
        j += 1
      }
      i += 1
    }
    this
  }

  def apply(i: Int, j: Int) : Distance = data(i)(j)

  def update(i: Int, j: Int, elem: Int): Unit = data(i)(j) = elem

  def padTo(n: Int, elem: Distance): Matrix = {
    val newData = data.map(_.padTo(n, elem)).padTo(n, Array.fill[Distance](n)(elem))
    new Matrix(newData)
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Matrix]

  override def equals(other: Any): Boolean = other match {
    case that: Matrix =>
      (that canEqual this) &&
        dim == that.dim &&
        data.map(_.toSeq).toSeq == that.data.map(_.toSeq).toSeq
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(dim, data)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    "\n"+
    data.map(
      row => "| " + row.map(
        elem => if(elem == Infinity) "∞" else "%3f".format(elem))
        .mkString(" ") + " |")
      .mkString("\n") + "\n"
  }
}

object MatrixHelpers {

  def symmetricZeroDiagonalRaw(dim: Int): Array[Array[Distance]] = {
    val data = Array.ofDim[Distance](dim, dim)
    var i = 0
    while(i<dim) {
      var j = 0
      while(j<dim) {
        if(i==j) {
          data(i)(j) = 0
        } else {
          val elem =
            if(Random.nextDouble() < 0.25)
              Infinity
            else
              Random.nextFloat()
          data(i)(j) = elem
          data(j)(i) = elem
        }
        j += 1
      }
      i += 1
    }
    data
  }

  def symmetricZeroDiagonal(dim: Int): Matrix =
    new Matrix(symmetricZeroDiagonalRaw(dim))

  def hasZeroDiagonal(matrix: Matrix): Boolean = {
    var i = 0
    while(i<matrix.dim) {
      if(matrix(i,i) != 0f)
        return false
      i += 1
    }
    return true
  }

  def deepClone(data: Array[Array[Distance]]): Array[Array[Distance]] =
    data.map(_.clone())

}

