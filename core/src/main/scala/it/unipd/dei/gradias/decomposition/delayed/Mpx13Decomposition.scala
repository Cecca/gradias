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

package it.unipd.dei.gradias.decomposition.delayed

import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.util.{Utils, Statistics}
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

object Mpx13Decomposition {

  val persistLevel = Utils.storageLevel

  def decomposition(
                     graph: RDD[(NodeId, Array[Int])],
                     beta: Double,
                     targetFraction: Double)
  : RDD[(NodeId, Mpx13Vertex)] = {
    val numNodes = graph.count()
    algoLogger.info("Loaded graph with {} nodes", numNodes)
    val originalGraph: RDD[(NodeId, Mpx13Vertex)] = action("Original graph creation") {
      initDelays(graph, beta)
        .partitionBy(new HashPartitioner(graph.sparkContext.defaultParallelism))
        .persist(persistLevel)
    }
    printDelays(originalGraph)

    val targetSize = (originalGraph.count * targetFraction).toInt

    val res = action("decomposition") {
      val _res = iterations(0, originalGraph, targetSize)
      val _fixed = fixUncovered(_res).setName("Final result").persist(persistLevel)
      _res.unpersist()
      _fixed
    }

    res
  }

  def fixUncovered(graph: RDD[(NodeId, Mpx13Vertex)]): RDD[(NodeId, Mpx13Vertex)] = {
    graph.map {
      case (id, v) =>
        if(v.isUncovered)
          (id, Mpx13Vertex.makeCenter(id, v))
        else
          (id, v)
    }
  }

  def initDelays(rawGraph: RDD[(NodeId, Array[NodeId])], beta: Double): RDD[(NodeId, Mpx13Vertex)] = {
    val expDelays = rawGraph.mapValues({ _ =>
      new Statistics.Exponential(beta).sample.toInt
    }).persist(persistLevel)
    val maxDelay = expDelays.values.reduce(math.max)
    expDelays.mapValues(d => maxDelay - d + 1) // + 1 to enable only the ones with original delay 0 in the first iteration
      .join(rawGraph)
      .mapValues({case (d, arr) => Mpx13Vertex(d, arr)})
  }

  def printDelays(graph: RDD[(NodeId, Mpx13Vertex)]) = {
    val delays = graph.values.map{
      case v: Mpx13Vertex => (v.delay, 1)
      case _ => throw new IllegalArgumentException("Called printDelayes on a graph with other types of nodes")
    }.reduceByKey(_ + _).collect().toMap
    algoLogger.info("\n"+Statistics.hist(delays))
  }

  val grow = new MessagingProtocol[Mpx13Vertex, (NodeId, Distance)](
    genMessages, mergeMessages, joinMessages, persistLevel)

  @tailrec
  def iterations(iteration: Int, partial: RDD[(NodeId, Mpx13Vertex)], targetSize: Long)
  : RDD[(NodeId, Mpx13Vertex)] = {

    val prevUncovered = countUncovered(partial)
    algoLogger.info("Contraction: iteration {}, {} uncovered nodes", iteration, prevUncovered)
    if(prevUncovered == 0){
      algoLogger.info("Graph completely covered, stopping")
      return partial
    }

    val decreasedDelays = action("Decrease delays"){
      partial.mapPartitions({ nodes =>
        nodes.map {
          case (id, v) => (id, v.decreaseDelay(id))
        }
      }, preservesPartitioning = true)
        .persist(persistLevel)
    }

    partial.unpersist()

    val updatedGraph = grow.run(decreasedDelays)

    decreasedDelays.unpersist()

    val curUncovered = countUncovered(updatedGraph)
    val centers = countCenters(partial)

    if(curUncovered + centers <= targetSize){
      return updatedGraph
    }

    iterations(iteration + 1, updatedGraph, targetSize)
  }

  def genMessages(id: NodeId, vertex: Mpx13Vertex): TraversableOnce[(NodeId, (NodeId, Distance))] =
    if (vertex.frontier)
      vertex.neighbourhood.map(v => (v, (vertex.center, vertex.distance + 1)))
    else
      Seq.empty

  def mergeMessages(a: (NodeId, Distance), b: (NodeId, Distance)): (NodeId, Distance) = {
    return a
  }

  def joinMessages(vertex: Mpx13Vertex, messages: Option[(NodeId, Distance)]) = messages match {
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

  def countUncovered(graph: RDD[(NodeId, Mpx13Vertex)]) : Long =
    graph.filter(_._2.isUncovered).count()

  def countCenters(graph: RDD[(NodeId, Mpx13Vertex)]) : Long =
    graph.filter(_._2.isCenter).count()

}



case class Mpx13Vertex(
                        center: NodeId,
                        distance: Distance,
                        frontier: Boolean,
                        delay: Int,
                        neighbourhood: Array[NodeId])
  extends CenterInfo with DistanceInfo with NeighbourhoodInfo with Serializable {

  def isCenter: Boolean = distance == 0.0f

  def isUncovered: Boolean = distance < 0

  def joinCluster(msg: (NodeId, Distance)): Mpx13Vertex =
    new Mpx13Vertex(msg._1, msg._2, true, delay, neighbourhood)

  /**
   * Removes the vertex from the frontier
   */
  def unfrontier: Mpx13Vertex = this.copy(frontier = false)

  def decreaseDelay(myId: NodeId): Mpx13Vertex = {
    if (isUncovered){
      val newDelay = delay - 1
      if (newDelay <= 0)
        this.copy(delay = newDelay, center = myId, frontier = true, distance = 0)
      else
        this.copy(delay = newDelay)
    }
    else
      this
  }

  override def toString: String =
    "DelayedVertex([%s], %d)"
      .format(neighbourhood.mkString(", "), delay)

}

object Mpx13Vertex extends Serializable {

  def apply(delay: Int, neighbours: Array[NodeId]): Mpx13Vertex =
    new Mpx13Vertex(-1, -1, false, delay, neighbours)

  def makeCenter(id: NodeId, vertex: Mpx13Vertex): Mpx13Vertex =
    vertex.copy(center = id, distance = 0, frontier = true)

}
