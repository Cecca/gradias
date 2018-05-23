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

package it.unipd.dei.gradias.diameter.dijkstra

import it.unipd.dei.gradias.{Distance, Infinity}
import it.unipd.dei.gradias.diameter.{CSCLocalGraphBuilder, Matrix}
import it.unipd.dei.gradias.diameter.MatrixHelpers._
import org.scalatest.{FreeSpec, Matchers}

class DijkstraAPSPSpec extends FreeSpec with Matchers {

  def doTest(dim: Int) = {
    val data = symmetricZeroDiagonalRaw(dim)

    val actual = Dijkstra.apsp(new Matrix(deepClone(data)))

    val expected = new Matrix(deepClone(data)).floydWarshall()

    assert(actual.isSymmetric, s"$actual")
    actual should equal (expected)
  }

  def doTestCSC(dim: Int) = {
    val data = symmetricZeroDiagonalRaw(dim)

    val cscBuild = new CSCLocalGraphBuilder()
    val n = data.length
    var i = 0
    while(i < n) {
      var j = 0
      while(j < n) {
        if (data(i)(j) != Infinity) {
          cscBuild.add(i, j, data(i)(j))
        }
        j += 1
      }
      i += 1
    }

    val csc = cscBuild.freeze()

    val results = Array.ofDim[Array[Distance]](n)
    i = 0
    while(i < n) {
      results(i) = Dijkstra.sssp(i, csc)
      i += 1
    }

    val actual = new Matrix(results)

    val expected = new Matrix(deepClone(data)).floydWarshall()

    assert(actual.isSymmetric, s"$actual")
    actual should equal (expected)
  }

  "Dijkstra must give the same results as Floyd-Warshall" - {

    "Matrix dimension = 1" in { doTest(1) }
    "Matrix dimension = 2" in { doTest(2) }
    "Matrix dimension = 4" in { doTest(4) }
    "Matrix dimension = 8" in { doTest(8) }
    "Matrix dimension = 16" in { doTest(16) }
    "Matrix dimension = 32" in { doTest(32) }
    "Matrix dimension = 64" in { doTest(64) }
    "Matrix dimension = 128" in { doTest(128) }

  }

  "Dijkstra (CSC) must give the same results as Floyd-Warshall" - {

    "Matrix dimension = 2" in { doTestCSC(2) }
    "Matrix dimension = 4" in { doTestCSC(4) }
    "Matrix dimension = 8" in { doTestCSC(8) }
    "Matrix dimension = 16" in { doTestCSC(16) }
    "Matrix dimension = 32" in { doTestCSC(32) }
    "Matrix dimension = 64" in { doTestCSC(64) }
    "Matrix dimension = 128" in { doTestCSC(128) }

  }

}
