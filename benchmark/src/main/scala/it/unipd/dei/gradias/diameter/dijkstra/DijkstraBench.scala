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

import java.io.File

import it.unipd.dei.gradias.diameter.MatrixHelpers
import org.scalameter.Key._
import org.scalameter._

object DijkstraBench extends PerformanceTest.OfflineReport {

  override val persistor = persistence.SerializationPersistor(new File("tmp"))

  val sizes = Gen.exponential("size")(64, 1024, 2)
  val matrices = for (sz <- sizes) yield MatrixHelpers.symmetricZeroDiagonal(sz)
  val graphs = for(m <- matrices) yield DijkstraGraph.matrixToDijkstraGraph(m)

  performance of "APSP" config(
    reports.resultDir -> "tmp",
    exec.independentSamples -> 1,
    verbose -> false,
    exec.maxWarmupRuns -> 10
    )  in {

    measure method "Floyd Warshall" in {

      using(matrices) curve "baseline" in { m =>
        m.floydWarshall()
      }
    }

    measure method "Dijkstra" in {

      using(graphs) curve "complete" in { g =>
        Dijkstra.apsp(g)
      }
      using(graphs) curve "single" in { g =>
        Dijkstra.sssp(0, g)
      }
    }
  }

}

object DijkstraRegressionBench extends PerformanceTest.OfflineReport {

  override val persistor = persistence.SerializationPersistor(new File("tmp"))

  val sizes = Gen.exponential("size")(16, 128, 2)
  val matrices = for (sz <- sizes) yield MatrixHelpers.symmetricZeroDiagonal(sz)
  val graphs = for(m <- matrices) yield DijkstraGraph.matrixToDijkstraGraph(m)

  performance of "Dijkstra" config(
    reports.resultDir -> "tmp",
    exec.independentSamples -> 1,
    verbose -> false
    ) in {

    measure method "apsp" in {

      using(graphs) curve "complete" in { g =>
        Dijkstra.apsp(g)
      }
    }
  }

}
