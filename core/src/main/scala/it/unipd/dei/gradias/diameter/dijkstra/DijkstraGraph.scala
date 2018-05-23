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

import it.unipd.dei.gradias.util.RunningStats
import it.unipd.dei.gradias.{Distance, NodeId, Infinity}
import it.unipd.dei.gradias.diameter.Matrix

/**
 * Store information on the graph as two two-dimensional arrays, one for the
 * weights and one for the adjacency. Note that these two-dimensional arrays
 * are not matrices, in the sense that the inner arrays have different lengths.
 *
 * This class is used in place of a single double array of tuples to limit
 * memory allocation and object creation. This way the overhead of object
 * creation for tuples in the inner loop of Dijkstra's algorithm is avoided
 * and so is the garbage collection of theses objects. This leads to better
 * performance.
 */
class DijkstraGraph(
                     val weights: Array[Array[Distance]],
                     val adjacency: Array[Array[NodeId]])
  extends Serializable {

  lazy val stats = {
    val weightStats = new RunningStats()
    val degreeStats = new RunningStats()

    var i = 0
    while(i < size) {
      var j = 0
      while (j < weights(i).length) {
        if (i < adjacency(i)(j)) {
          weightStats.push(weights(i)(j))
        }
        degreeStats.push(adjacency(i).length)
        j += 1
      }
      i += 1
    }

    Map(
      "nodes" -> size,
      "edges" -> weightStats.count,
      "degree-mean" -> degreeStats.mean,
      "degree-variance" -> degreeStats.variance,
      "degree-stddev" -> degreeStats.stddev,
      "degree-max" -> degreeStats.max,
      "degree-min" -> degreeStats.min,
      "weight-mean" -> weightStats.mean,
      "weight-variance" -> weightStats.variance,
      "weight-stddev" -> weightStats.stddev,
      "weight-max" -> weightStats.max,
      "weight-min" -> weightStats.min
    )
  }

  def size: Int = weights.length

  def averageDegree: Double = {
    val total = adjacency.map(_.length).sum
    total / this.size.toDouble
  }

  def maxWeight: Distance =
    weights.map(_.max).max

}

object DijkstraGraph {

  def matrixToDijkstraGraph(matrix: Matrix): DijkstraGraph = {
    val grouped = matrix.data.zipWithIndex.map{ case (row, id) =>
      val adj = row.zipWithIndex.filter(_._1 != Infinity)
      adj:Seq[(Distance, NodeId)]
    }
    val weights = grouped.map(_.map(_._1).toArray)
    val ids = grouped.map(_.map(_._2).toArray)
    new DijkstraGraph(weights, ids)
  }


}
