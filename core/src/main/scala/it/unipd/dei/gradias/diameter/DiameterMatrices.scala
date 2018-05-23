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

import it.unipd.dei.gradias._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import it.unipd.dei.gradias.util.Logger.algoLogger

import scala.reflect.ClassTag


class DiameterMatrices(val adjacency: Matrix, val centersRadius: Matrix)

object DiameterMatrices extends Serializable {

  def leanGraphToMatrices[V <: CenterInfo with DistanceInfo](nodes: RDD[(NodeId, V)], edges: RDD[(EdgeId, Distance)])
  : DiameterMatrices = {
    val conv = new LeanGraphToMatrices(nodes, edges)
    new DiameterMatrices(conv.adjacencyMatrix, conv.centersRadiusMatrix)
  }

  class LeanGraphToMatrices[V <: CenterInfo with DistanceInfo](
                                                                _nodes: RDD[(NodeId, V)],
                                                                edges: RDD[(EdgeId, Distance)])
    extends Serializable {

    algoLogger.info("Converting result to matrices")

    val centers: RDD[(NodeId, Distance)] = _nodes.map {
      case (id, v) => (v.center, v.distance)
    }.reduceByKey(math.max)

    def remapEdges(
                    weightsMap: Array[(EdgeId, Distance)],
                    idRemapping: Map[NodeId, NodeId])
    : Array[(EdgeId, Distance)] = {
      algoLogger.info("Remapping edges")
      val m = idRemapping
      weightsMap.map{case ((u,v),w) => ((m(u), m(v)), w)}
    }

    val idRemapping = {
      algoLogger.info("Remapping IDs")
      val m = scala.collection.mutable.Map[NodeId, NodeId]()
      var cnt = 0
      for(u <- centers.keys.collect()) {
        if(!m.contains(u)) {
          m.update(u, cnt)
          cnt += 1
        }
      }
      m.toMap
    }

    lazy val numNodes = idRemapping.size

    val adjacencyMatrix = {
      algoLogger.info("Building adjacency matrix")
      val weightsMap = remapEdges(edges.collect, idRemapping)
      val matrix = Array.fill[Distance](numNodes,numNodes)(Infinity)
      for(((u,v),w) <- weightsMap) {
        if(u == v) {
          matrix(u)(v) = 0
        } else {
          matrix(u)(v) = w
          matrix(v)(u) = w
        }
      }
      for (i <- 0 until matrix.length) {
        matrix(i)(i) = 0
      }
      val mat = new Matrix(matrix)
      assert(mat.isSymmetric)
      mat
    }

    val centersRadiusMatrix = {
      algoLogger.info("Building centers radius matrix")
      val radiusMap = centers.collect.map {
        case (center, radius) => (idRemapping(center), radius)
      }.toMap
      val matrix = Array.fill[Distance](numNodes,numNodes)(0)
      var i = 0
      while (i<numNodes) {
        var j = 0
        while (j < numNodes) {
          matrix(i)(j) = radiusMap(i) + radiusMap(j)
          j += 1
        }
        i += 1
      }
      val mat = new Matrix(matrix)
      assert(mat.isSymmetric)
      mat
    }
  }

}