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

package it.unipd.dei.gradias.bfs

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.util.Utils
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Bfs {

  def apply(experiment: Experiment): Bfs = new Bfs(experiment)

  val persistLevel = Utils.storageLevel

}

class Bfs(@transient experiment: Experiment) extends Serializable {
  import it.unipd.dei.gradias.bfs.Bfs.persistLevel

  def apply(graph: RDD[(NodeId, Array[NodeId])]): Int = {
    val seed: NodeId = graph.keys.takeSample(false, 1)(0)

    algoLogger.info(s"Starting BFS from node $seed")
    experiment.tag("starting node", seed)

    val _graph: RDD[(NodeId, BfsVertex)] = action("Input creation") {
      graph.map { case (id, neighs) =>
        if(id == seed) {
          (id, new BfsVertex(true, true, neighs))
        } else {
          (id, new BfsVertex(false, false, neighs))
        }
      }
        .partitionBy(new HashPartitioner(graph.sparkContext.defaultParallelism))
        .persist(persistLevel).setName("Initial graph")
    }
    graph.unpersist()
    run(_graph)
  }

  def run(graph: RDD[(NodeId, BfsVertex)]): Int = action("bfs") {

    val grow = new MessagingProtocol(
      genMessages, mergeMessages, joinMessages, persistLevel)

    var _graph: RDD[(NodeId, BfsVertex)] = graph

    var iteration = 0
    var unreached = countUnreached(_graph)

    while(unreached > 0) {

      algoLogger.info(s"Iteration $iteration: still $unreached nodes")

      val start = System.currentTimeMillis()

      val updatedGraph = grow.run(_graph, checkpoint = iteration % 20 == 0 && iteration > 0)

      _graph.unpersist()
      _graph = updatedGraph

      iteration += 1
      unreached = countUnreached(_graph)
      val end = System.currentTimeMillis()
      val time = end - start
      experiment.append("iterations",
        "iteration", iteration: java.lang.Integer,
        "time", time: java.lang.Long)
    }

    iteration
  }

  def countUnreached(graph: RDD[(NodeId, BfsVertex)]): Long = {
    graph.filter(! _._2.reached).count()
  }

  def genMessages(id: NodeId, vertex: BfsVertex): TraversableOnce[(NodeId, Boolean)] = {
    if (vertex.frontier)
      vertex.neighbourhood.map((_, true)) :+ (id, true)
    else
      Seq()
  }

  def mergeMessages(a: Boolean, b: Boolean): Boolean = a

  def joinMessages(vertex: BfsVertex, message: Option[Boolean]) = message match {
    case Some(msg) =>
      if (vertex.reached)
        new BfsVertex(true, false, vertex.neighbourhood)
      else
        new BfsVertex(true, true, vertex.neighbourhood)
    case None => vertex
  }

}

case class BfsVertex(reached: Boolean, frontier: Boolean, neighbourhood: Array[NodeId])
  extends NeighbourhoodInfo