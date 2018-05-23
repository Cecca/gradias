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

import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias.util.Logger.algoLogger
import org.apache.spark.graphx
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object BiggestConnectedComponent {

  def apply(adjRdd: RDD[(NodeId, Array[NodeId])]): RDD[(Int, Int)] = {
    val graphx = adjListToGraphx(adjRdd)
    val bcc = compute(graphx)
    graphxToEdges(bcc)
  }

  def adjListToGraphx(adjRdd: RDD[(NodeId, Array[NodeId])]) = {
    val res = action("adj to graphx conversion") {
      val edges = adjRdd.flatMap { case (id, neighs) =>
        neighs.filter(_ > id).map(v => Edge(id, v))
      }
      // Dummy type values
      graphx.Graph.fromEdges[Byte, Nothing](edges, 0.toByte,
        vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER,
        edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER).cache()
    }
    adjRdd.unpersist()
    res
  }

  private def graphxToEdges(graph: graphx.Graph[VertexId, Nothing]) ={
    val res = action("graphx to edges conversion") {
      graph.edges.map { e =>
        (e.srcId.toInt, e.dstId.toInt)
      }.cache()
    }
    graph.unpersistVertices()
    graph.edges.unpersist()
    res
  }

  private def compute(graph: graphx.Graph[Byte,Nothing]): graphx.Graph[VertexId, Nothing] = action("compute bcc") {
    val ccs: graphx.Graph[VertexId, Nothing] = action("connected components computation"){
      graph.ops.connectedComponents().cache()
    }
    graph.unpersistVertices()
    graph.edges.unpersist()
    val ccSizes = ccs.vertices.map(_.swap).countByKey()
    val biggest = ccSizes.maxBy(_._2)
    algoLogger.info(s"The biggest component has size ${biggest._2}")
    val biggestId = biggest._1
    val out = ccs.subgraph(
      vpred = (vId, cId) => cId == biggestId).cache()
    algoLogger.info(s"Output graph has ${out.ops.numVertices} nodes")
//    ccs.unpersistVertices()
//    ccs.edges.unpersist()
    out
  }

}
