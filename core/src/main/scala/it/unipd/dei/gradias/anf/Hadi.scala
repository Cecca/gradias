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

package it.unipd.dei.gradias.anf

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.NodeId
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.Utils
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Hadi {

  val persistLevel = Utils.storageLevel

  def apply(experiment: Experiment): Hadi = {
    new Hadi(experiment)
  }

}

class Hadi(@transient experiment: Experiment) extends Serializable {
  import it.unipd.dei.gradias.anf.Hadi._

  def run (
            graph: RDD[(NodeId, Array[NodeId])],
            numCounters: Int): Int = {

    var g = init(graph, numCounters)
    graph.unpersist()

    var numChanged = g.count()
    var it = 0

    while(numChanged > 0) {
      val start = System.currentTimeMillis()
      val (updated, changed) = iteration(it, g, checkpoint = it % 20 == 0 && it > 0)
      g.unpersist()
      g = updated
      numChanged = changed
      it += 1
      val end = System.currentTimeMillis()
      val time = end - start
      experiment.append("iterations",
        "iteration", it: java.lang.Integer,
        "time", time: java.lang.Long)
    }

    // the number of iterations is the diameter
    return it
  }

  def init(
            graph: RDD[(NodeId, Array[NodeId])],
            numCounters: Int): RDD[(NodeId, HadiVertex)] = action("init") {
    graph.map { case (id, neighs) =>
      (id, new HadiVertex(ProbabilisticCounter(numCounters), neighs))
    }.setName("Remapped Input Graph").persist(persistLevel)
  }

  def iteration(itNum: Int, graph: RDD[(NodeId, HadiVertex)], checkpoint: Boolean = false) =
    action(s"iteration $itNum") {
      val messages = action("message generation") {
        graph.flatMap { case (id, v) => v.sendCounter() }
          .reduceByKey(_ union _)
          .setName(s"messages ($itNum)").persist(persistLevel)
      }
      val changed = graph.sparkContext.accumulator(0L)
      val updated = action("update graph") {
        val _g = graph.leftOuterJoin(messages).mapValues {
          case (v, Some(cnt)) =>
            if (v.counter != cnt) {
              changed += 1
              v.copy(counter = v.counter union cnt)
            } else {
              v
            }
          case (v, None) => v
        }.setName("update graph").persist(persistLevel)
        if(checkpoint) {
          algoLogger.info(yellow("___checkpoint___"))
          _g.checkpoint()
        }
        _g
      }
      messages.unpersist()
      (updated, changed.value)
    }

  case class HadiVertex(counter: ProbabilisticCounter, neighbours: Array[NodeId]) extends Serializable {

    def sendCounter(): TraversableOnce[(NodeId, ProbabilisticCounter)] = {
      neighbours.map((_, counter))
    }

  }

}
