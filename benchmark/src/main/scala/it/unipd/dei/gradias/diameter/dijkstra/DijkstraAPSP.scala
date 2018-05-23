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

import it.unipd.dei.gradias.{Distance, Infinity, NodeId}
import it.unipd.dei.gradias.diameter.Matrix

import scala.collection.mutable

/**
 * Fast implementation of Dijkstra's algorithm moved to the core
 * project. Slow implementations using the standard library priority
 * queue kept here for reference.
 */
object DijkstraAPSP {

  def ssspSlow(src: NodeId, graph: DijkstraGraph): Array[Distance] = {
    val ordering = Ordering.by[(Distance,Int), Distance](_._1).reverse

    val visited = Array.fill(graph.size)(false)
    val q = mutable.PriorityQueue[(Distance, NodeId)]((0, src))(ordering)
    val distance = Array.fill(graph.size)(Infinity)
    distance(src) = 0

    while(q.nonEmpty) {
      val (_, vertex) = q.dequeue()
      if(!visited(vertex)){
        visited(vertex) = true
        val nWeights = graph.weights(vertex)
        val nIds = graph.adjacency(vertex)
        var i = 0
        while (i < nWeights.length) {
          val w = nWeights(i)
          val v = nIds(i)
          val d = w + distance(vertex)
          if(d < distance(v)) {
            distance(v) = d
            if(!visited(v)){
              q.enqueue((d, v))
            }
          }
          i += 1
        }
      }
    }

    distance
  }

  def apspSlow(matrix: Matrix): Matrix = {

    val graph = DijkstraGraph.matrixToDijkstraGraph(matrix)
    val distances = apspSlow(graph)

    new Matrix(distances)
  }

  def apspSlow(graph: DijkstraGraph): Array[Array[Distance]] = {
    val result = Array.ofDim[Array[Distance]](graph.size)
    var i = 0
    while(i < graph.size) {
      result(i) = ssspSlow(i, graph)
      i += 1
    }
    result
  }

}
