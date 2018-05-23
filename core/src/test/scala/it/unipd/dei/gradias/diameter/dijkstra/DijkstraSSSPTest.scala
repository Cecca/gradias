package it.unipd.dei.gradias.diameter.dijkstra

import it.unipd.dei.gradias.Infinity
import it.unipd.dei.gradias.diameter.{CSCLocalGraphBuilder, Matrix}
import it.unipd.dei.gradias.diameter.MatrixHelpers._
import org.scalatest.{Matchers, FreeSpec}

import scala.util.Random

class DijkstraSSSPTest extends FreeSpec with Matchers {

  def doTest(dim: Int) = {
    val data = symmetricZeroDiagonalRaw(dim)
    val src = Random.nextInt(data.length)

    val expected = Dijkstra.sssp(src, DijkstraGraph.matrixToDijkstraGraph(new Matrix(deepClone(data))))

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

    val actual = Dijkstra.sssp(src, csc)

    actual.toList should equal (expected.toList)
  }

  "Both implementations of Dijkstra should give the same result" - {

    "Graph dimension = 2" in { doTest(2) }
    "Graph dimension = 4" in { doTest(4) }
    "Graph dimension = 8" in { doTest(8) }
    "Graph dimension = 16" in { doTest(16) }
    "Graph dimension = 32" in { doTest(32) }
    "Graph dimension = 64" in { doTest(64) }
    "Graph dimension = 128" in { doTest(128) }

  }

}
