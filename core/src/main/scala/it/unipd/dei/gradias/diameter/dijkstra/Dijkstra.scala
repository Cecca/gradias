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

import it.unipd.dei.gradias._
import it.unipd.dei.gradias.diameter.{CSCLocalGraph, Matrix}
import it.unipd.dei.gradias.util.Logger._
import org.apache.spark.SparkContext

object Dijkstra {

  def sssp(src: NodeId, graph: DijkstraGraph): Array[Distance] = {

    val q = new PriorityQueue(graph.size)
    val distance = Array.fill[Distance](graph.size)(Infinity)
    distance(src) = 0

    for(v <- Range(0, graph.size)) {
      q.enqueue(v, Infinity)
    }

    q.decreasePriority(src, 0)

    while(q.nonEmpty) {
      val vertex = q.dequeue()
      val nWeights = graph.weights(vertex)
      val nIds = graph.adjacency(vertex)
      var i = 0
      while(i < nWeights.length) {
        val w = nWeights(i)
        val v = nIds(i)
        val d = Distance.sum(w, distance(vertex))
        if (d < 0)
          throw new IntegerOverflowException(
            s"Vertex $vertex, distance ${Distance.toString(distance(vertex))}, " +
              s"neighbour $v, weight $w, sum $d")
        if(d < distance(v)) {
          distance(v) = d
          q.decreasePriority(v, d)
        }
        i += 1
      }
    }

    distance
  }

  def sssp(src: NodeId, graph: CSCLocalGraph): Array[Distance] = {

    val q = new PriorityQueue(graph.n)
    val distance = Array.fill[Distance](graph.n)(Infinity)
    distance(src) = 0

    for(v <- Range(0, graph.n)) {
      q.enqueue(v, Infinity)
    }

    q.decreasePriority(src, 0)

    while(q.nonEmpty) {
      val vertex = q.dequeue()
      val pStart = graph.columnPointers(vertex)
      val pEnd = graph.columnPointers(vertex+1)
      var i = pStart
      while(i < pEnd) {
        val w = graph.weights(i)
        val v = graph.rowIndices(i)
        val d = Distance.sum(w, distance(vertex))
        if (d < 0)
          throw new IntegerOverflowException(
            s"Vertex $vertex, distance ${Distance.toString(distance(vertex))}, " +
              s"neighbour $v, weight $w, sum $d")
        if(d < distance(v)) {
          distance(v) = d
          q.decreasePriority(v, d)
        }
        i += 1
      }
    }

    distance
  }

  def apsp(graph: DijkstraGraph): Array[Array[Distance]] = {
    val result = Array.ofDim[Array[Distance]](graph.size)
    var i = 0
    while(i < graph.size) {
      result(i) = sssp(i, graph)
      i += 1
    }
    result
  }

  def apsp(graph: CSCLocalGraph): Array[Array[Distance]] = {
    val result = Array.ofDim[Array[Distance]](graph.n)
    var i = 0
    while(i < graph.n) {
      result(i) = sssp(i, graph)
      i += 1
    }
    result
  }

  def apsp(sc: SparkContext, graph: CSCLocalGraph): Array[Array[Distance]] = {
    algoLogger.info("Broadcasting graph")
    val dg = sc.broadcast(graph)
    algoLogger.info("Create empty distance graph")
    val ids = sc.parallelize(Range(0, graph.n))
    algoLogger.info("Compute distances using Dijkstra's algorithm")
    val distances =
      ids.map { id =>
        (id, Dijkstra.sssp(id, dg.value))
      }.collect().sortBy(_._1).map(_._2)
    distances
  }

  def apsp(matrix: Matrix): Matrix = {

    val graph = DijkstraGraph.matrixToDijkstraGraph(matrix)
    val distances = apsp(graph)

    new Matrix(distances)
  }

}

class IntegerOverflowException(message: String) extends Exception(message)