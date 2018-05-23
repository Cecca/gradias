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

import it.unipd.dei.gradias.diameter.MatrixHelpers._
import org.scalatest.FreeSpec
import it.unipd.dei.gradias.{Distance, Infinity}

import scala.util.Random

class MatrixSpec extends FreeSpec {

  "Given an integer symmetric matrix with a diagonal of zeroes" - {
    "The closure wrt the minplus product should have zeroes on the diagonal" - {
      "dim = 4" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(4).closure))
      }
      "dim = 16" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(16).closure))
      }
      "dim = 32" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(32).closure))
      }
      "dim = 64" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(64).closure))
      }
      "dim = 128" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(128).closure))
      }
      "dim = 256" in {
        assert(hasZeroDiagonal(symmetricZeroDiagonal(256).closure))
      }
    }
  }

  "Given a symmetric matrix of ones with zeroes on the diagonal" - {
    "The minplus closure should yield the correct distances" in {
      val m = new Matrix(
        Array(
          Array(0,1,1,1,1,1,1,1,1,Infinity),
          Array(1,0,1,Infinity,1,Infinity,1,Infinity,Infinity,Infinity),
          Array(1,1,0,Infinity,Infinity,1,Infinity,1,Infinity,1),
          Array(1,Infinity,Infinity,0,1,Infinity,1,1,Infinity,Infinity),
          Array(1,1,Infinity,1,0,1,Infinity,1,Infinity,1),
          Array(1,Infinity,1,Infinity,1,0,1,Infinity,1,1),
          Array(1,1,Infinity,1,Infinity,1,0,1,1,1),
          Array(1,Infinity,1,1,1,Infinity,1,0,1,1),
          Array(1,Infinity,Infinity,Infinity,Infinity,1,1,1,0,1),
          Array(Infinity,Infinity,1,Infinity,1,1,1,1,1,0)))

      val expected = new Matrix(
        Array(
           Array( 0,  1,  1,  1,  1,  1,  1,  1,  1,  2),
           Array( 1,  0,  1,  2,  1,  2,  1,  2,  2,  2),
           Array( 1,  1,  0,  2,  2,  1,  2,  1,  2,  1),
           Array( 1,  2,  2,  0,  1,  2,  1,  1,  2,  2),
           Array( 1,  1,  2,  1,  0,  1,  2,  1,  2,  1),
           Array( 1,  2,  1,  2,  1,  0,  1,  2,  1,  1),
           Array( 1,  1,  2,  1,  2,  1,  0,  1,  1,  1),
           Array( 1,  2,  1,  1,  1,  2,  1,  0,  1,  1),
           Array( 1,  2,  2,  2,  2,  1,  1,  1,  0,  1),
           Array( 2,  2,  1,  2,  1,  1,  1,  1,  1,  0)))

      val actual = m.closure
      assert(actual == expected, "Was\n" + actual)
    }
  }

  "The closure algorithm and the floyd warshall one should give the very same result" - {
    "for 4x4 matrices" in {
      val dim = 4
      val adj = Array.ofDim[Distance](dim,dim)

      for(i <- 0 until dim; j <- 0 until i) {
        if(i == j) {
          adj(i)(j) = 0
        } else {
          val v = if(Random.nextDouble() < 0.25) {
            Infinity
          } else {
            Random.nextFloat()
          }
          adj(i)(j) = v
          adj(j)(i) = v
        }
      }

      val fw = new Matrix(adj.clone()).floydWarshall()
      val mm = new Matrix(adj.clone()).closure

      assert(fw == mm)
    }

    "for 16x16 matrices" in {
      val dim = 16
      val adj = Array.ofDim[Distance](dim,dim)

      for(i <- 0 until dim; j <- 0 until i) {
        if(i == j) {
          adj(i)(j) = 0
        } else {
          val v = if(Random.nextDouble() < 0.25) {
            Infinity
          } else {
            Random.nextFloat()
          }
          adj(i)(j) = v
          adj(j)(i) = v
        }
      }

      val fw = new Matrix(adj.clone()).floydWarshall()
      val mm = new Matrix(adj.clone()).closure

      assert(fw == mm)
    }

    "for 99x99 matrices" in {
      val dim = 99
      val adj = Array.ofDim[Distance](dim,dim)

      for(i <- 0 until dim; j <- 0 until i) {
        if(i == j) {
          adj(i)(j) = 0
        } else {
          val v = if(Random.nextDouble() < 0.25) {
            Infinity
          } else {
            Random.nextFloat()
          }
          adj(i)(j) = v
          adj(j)(i) = v
        }
      }

      val fw = new Matrix(adj.clone()).floydWarshall()
      val mm = new Matrix(adj.clone()).closure

      assert(fw == mm)
    }

    "for 1050x1050 matrices" in {
      val dim = 1050
      val adj = Array.ofDim[Distance](dim,dim)

      for(i <- 0 until dim; j <- 0 until i) {
        if(i == j) {
          adj(i)(j) = 0
        } else {
          val v = if(Random.nextDouble() < 0.25) {
            Infinity
          } else {
            Random.nextFloat()
          }
          adj(i)(j) = v
          adj(j)(i) = v
        }
      }

      val fw = new Matrix(adj.clone()).floydWarshall()
      val mm = new Matrix(adj.clone()).closure

      assert(fw == mm)
    }
  }

}
