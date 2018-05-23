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

package it.unipd.dei.gradias.decomposition.cluster

import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.Utils
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object ClusterDecomposition {

  val persistLevel = Utils.storageLevel

  def decomposition(
                     graph: RDD[(NodeId, Array[NodeId])],
                     targetFraction: Double): RDD[(NodeId, ClusterVertex)] = {

    graph.persist(persistLevel)
    val numNodes = graph.count()
    val targetSize = math.ceil(numNodes * targetFraction).toLong

    algoLogger.info("Target size is {}", targetSize)

    val centersSeq = numCentersSeq(numNodes, targetSize)

    val inputGraph = action("Input conversion") {
      graph.mapValues(ClusterVertex.apply(_))
        .partitionBy(new HashPartitioner(graph.sparkContext.defaultParallelism))
        .setName("Input graph").persist(persistLevel)
    }

    val result = action("decomposition") {
      val _res = iterations(0, inputGraph, targetSize, centersSeq)
      val _fixed = fixUncovered(_res).setName("Final result").persist(persistLevel)
      _res.unpersist()
      _fixed
    }

    result
  }

  def numCentersSeq(numNodes: Long, targetSize: Long): Seq[Double] = {
    def numIt(n: Long): Int = {
      if(n <= targetSize)
        return 0
      else
        return 1 + numIt(n / 2)
    }
    val numIterations = numIt(numNodes)
    println("Number of iterations is " + numIterations)
    Array.fill(numIterations)(targetSize / numIterations.toDouble)
  }


  def iterations(
                  it: Int,
                  partial: RDD[(NodeId, ClusterVertex)],
                  targetSize: Long,
                  numCentersSeq: Seq[Double]): RDD[(NodeId, ClusterVertex)] = {

    val uncovered = countUncovered(partial)
    val centers = countCenters(partial)

    algoLogger.info(
      s"Start iteration $it. " +
        s"( ${green(targetSize)} | ${red(uncovered)} | ${yellow(centers)} ) "+
        "(target | uncovered | centers)")

    if(uncovered + centers <= targetSize || numCentersSeq.isEmpty) {
      return partial
    }

    val centerProbability = numCentersSeq.head / uncovered

    val wCenters = action("Center selection"){
      selectCenters(partial, centerProbability)
        .setName("Graph with new centers").persist(persistLevel)
    }

    partial.unpersist()

    val updatedPartial = iteration(it, wCenters, math.max(uncovered / 2, targetSize))

    wCenters.unpersist()

    iterations(it + 1, updatedPartial, targetSize, numCentersSeq.tail)
  }

  // - expand clusters as the simple BFS
  // - messages are arrays of two elements:
  //      [center id, distance]
  // - ties are broken by choosing an arbitrary center
  def iteration(
                 it: Int,
                 partial: RDD[(NodeId, ClusterVertex)],
                 target: Long): RDD[(NodeId, ClusterVertex)] = {

    val grow = new MessagingProtocol[ClusterVertex, (NodeId, Distance)](
      genMessages, mergeMessages, joinMessages, persistLevel)

    var _graph = partial
    var _uncovered = countUncovered(_graph)
    val centers = countCenters(partial)

    while (_uncovered + centers > target) {
      algoLogger.info("Still {} nodes uncovered", _uncovered)

      val updatedGraph = grow.run(_graph)

      _graph.unpersist()
      _graph = updatedGraph
      _uncovered = countUncovered(_graph)
    }

    algoLogger.info("Uncovered nodes and centers {} < {} target", _uncovered + centers, target)

    _graph
  }

  def fixUncovered(graph: RDD[(NodeId, ClusterVertex)]): RDD[(NodeId, ClusterVertex)] = {
    graph.map {
      case (id, v) =>
        if(v.isUncovered)
          (id, ClusterVertex.makeCenter(id, v.neighbourhood))
        else
          (id, v)
    }
  }

  def genMessages(id: NodeId, vertex: ClusterVertex): TraversableOnce[(NodeId, (NodeId, Distance))] =
    if (vertex.frontier)
      vertex.neighbourhood.map(v => (v, (vertex.center, vertex.distance + 1)))
    else
      Seq.empty

  def mergeMessages(a: (NodeId, Distance), b: (NodeId, Distance)): (NodeId, Distance) = {
    return a
  }

  def joinMessages(vertex: ClusterVertex, messages: Option[(NodeId, Distance)]) = messages match {
    case Some(msg) =>
      if (vertex.frontier) {
        vertex.unfrontier
      } else if (vertex.isUncovered) {
        vertex.joinCluster(msg)
      } else {
        vertex
      }
    case None =>
      if (vertex.frontier) {
        vertex.unfrontier
      }
      else {
        vertex
      }
  }

  def countUncovered(graph: RDD[(NodeId, ClusterVertex)]) : Long =
    graph.filter(_._2.isUncovered).count()

  def countCenters(graph: RDD[(NodeId, ClusterVertex)]) : Long =
    graph.filter(_._2.isCenter).count()

  def selectCenters(graph: RDD[(NodeId, ClusterVertex)], prob: Double): RDD[(NodeId, ClusterVertex)] = {
    graph.mapPartitions({ nodes =>
      nodes.map{case (id, v) =>
        if (v.isUncovered && Random.nextDouble() <= prob) {
          (id, ClusterVertex.makeCenter(id, v.neighbourhood))
        } else {
          (id, v)
        }
      }
    }, preservesPartitioning = true)
  }

}

/**
 *
 * @param center -1 signals that the node has not been captured
 * @param distance -1 signals that the node has not been captured
 * @param frontier true if in the next round the node should send its messages
 * @param neighbourhood the neighbourhood of the node
 */
case class ClusterVertex(
                          center: NodeId,
                          distance: Distance,
                          frontier: Boolean,
                          neighbourhood: Array[NodeId])
  extends CenterInfo with DistanceInfo with NeighbourhoodInfo with Serializable {

  def isCenter: Boolean = distance == 0

  def isUncovered: Boolean = distance < 0

  def joinCluster(msg: (NodeId, Distance)): ClusterVertex =
    new ClusterVertex(msg._1, msg._2, true, neighbourhood)

  /**
   * Removes the vertex from the frontier
   */
  def unfrontier: ClusterVertex = this.copy(frontier = false)

}

object ClusterVertex extends Serializable {

  def makeCenter(id: NodeId, neighbourhood: Array[NodeId]): ClusterVertex =
    new ClusterVertex(id, 0, true, neighbourhood)

  def apply(neighbourhood: Array[NodeId]): ClusterVertex =
    new ClusterVertex(-1, -1, false, neighbourhood)

}


