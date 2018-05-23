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

package it.unipd.dei.gradias.converter

import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.ArrayUtils
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import it.unipd.dei.gradias.util.Logger.algoLogger

object TailAdder {

  def addTails(
                graph: RDD[(NodeId, Array[NodeId])],
                tailLength: Int,
                numTails: Int) = {

    val maxId = graph.keys.reduce(math.max)
    algoLogger.info("Maximum ID is {}", maxId)

    val tails = (maxId+1).until(maxId + 1 + numTails*(tailLength+1))
      .grouped(tailLength+1)
      .map(s => s.head -> s.tail.toArray: (NodeId, Array[NodeId]))
      .toList

    val handles = graph.take(numTails)

    val newAdjs: Seq[(NodeId, Array[NodeId])] =
      tails.zip(handles).flatMap { case ((tHead, tail), (handle, _)) =>
        Seq( (tHead, Array(handle)), (handle, Array(tHead)) ) ++
          (tHead +: tail).zip(tail).flatMap { case (u, v) =>
            Seq((u,Array(v)), (v, Array(u)))
          }
      }

    val toAdd = graph.sparkContext.parallelize(newAdjs)

    graph.union(toAdd).reduceByKey(ArrayUtils.merge)
  }

}
