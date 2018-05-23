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

import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias.diameter.WeightedDiameter.Algorithm
import it.unipd.dei.gradias.diameter.dijkstra.{Dijkstra, DijkstraGraph}
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias._
import org.apache.spark.SparkContext

object WeightedDiameter {

  val algorithms = Set("FloydWarshall", "Dijkstra", "Distributed")

  type Algorithm = String

  def remapEdges(
                  weightsMap: Array[(EdgeId, Distance)],
                  idRemapping: Map[NodeId, NodeId])
  : Array[(EdgeId, Distance)] = {
    val m = idRemapping
    weightsMap.map{case ((u,v),w) => ((m(u), m(v)), w)}
  }

}

class WeightedDiameter(
                        sc: SparkContext,
                        matrices: DiameterMatrices,
                        algorithm: Algorithm) {

  val adjacencyMatrix = matrices.adjacency
  val centersRadiusMatrix = matrices.centersRadius

  val numNodes = adjacencyMatrix.data.size

  lazy val dijkstraGraph: DijkstraGraph =
    DijkstraGraph.matrixToDijkstraGraph(adjacencyMatrix)

  def floydWarshallDiameter = {
    adjacencyMatrix
      .floydWarshallSymmetric()
      .sum(centersRadiusMatrix)
      .max
  }

  def dijkstraDiameter = {
    new Matrix(Dijkstra.apsp(dijkstraGraph))
      .sum(centersRadiusMatrix)
      .max
  }

  def distributedDiameter = {
    algoLogger.info("Broadcasting graph")
    val dg = sc.broadcast(dijkstraGraph)
    algoLogger.info("Create empty distance graph")
    val ids = sc.parallelize(Range(0, numNodes))
    algoLogger.info("Compute distances using Dijkstra's algorithm")
    val distances =
      ids.map { id =>
        (id, Dijkstra.sssp(id, dg.value))
      }.collect().sortBy(_._1).map(_._2).toArray

    algoLogger.info("Computation completed, summing radiuses")
    new Matrix(distances)
      .sum(centersRadiusMatrix)
      .max
  }

  lazy val diameter: Distance = action("diameter") {
    algoLogger.info("Computing diameter with {} algorithm", algorithm)
    try {
      algorithm match {
        case "FloydWarshall" => floydWarshallDiameter
        case "Dijkstra" => dijkstraDiameter
        case "Distributed" => distributedDiameter
      }
    } catch {
      case e: Exception =>
        algoLogger.error("Diameter computation failed with an exception, dumping matrix.")
        Matrix.write(adjacencyMatrix, ".")
        throw e
    }
  }

}
