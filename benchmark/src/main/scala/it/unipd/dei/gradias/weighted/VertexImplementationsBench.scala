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

package it.unipd.dei.gradias.weighted

import it.unipd.dei.gradias.{Distance, Infinity, NodeId}
import org.scalameter._

import scala.util.Random

object VertexImplementationsBench extends PerformanceTest.OfflineReport {

  val numNeighbours = Gen.range("num neighbours")(10, 100, 10)
  val edges = for(n <- numNeighbours) yield (0 to n) zip (0 to n map (_ => Random.nextFloat()))
  val monolithic = for (es <- edges) yield MonolithicVertex(es)
  val traited = for (es <- edges) yield TraitedVertex(es)
  val interfaceWeighted = for (es <- edges) yield TraitedVertex(es): WeightedVertex
  val interfaceDistance = for (es <- edges) yield TraitedVertex(es): DistanceVertex
  val interfaceBucket = for (es <- edges) yield TraitedVertex(es): BucketVertex
  val bucket = Bucket(10, 0)

  performance of "Vertex_implementations" in {

    measure method "neighbours" in {

      using(monolithic) curve "monolithic class" in { v =>
        v.neighbourhood
      }

      using(traited) curve "traited class, direct calls" in { v =>
        v.neighbourhood
      }

      using(interfaceWeighted) curve "traited class, interface calls" in { v =>
        v.neighbourhood
      }

    }

    measure method "distance" in {

      using(monolithic) curve "monolithic class" in { v =>
        v.distance
      }

      using(traited) curve "traited class, direct calls" in { v =>
        v.distance
      }

      using(interfaceDistance) curve "traited class, interface calls" in { v =>
        v.distance
      }

    }

    measure method "isInto" in {

      using(monolithic) curve "monolithic class" in { v =>
        v.isInto(bucket)
      }

      using(traited) curve "traited class, direct calls" in { v =>
        v.isInto(bucket)
      }

      using(interfaceBucket) curve "traited class, interface calls" in { v =>
        v.isInto(bucket)
      }

    }

  }

}

trait WeightedVertex {

  def neighbourhood: Array[NodeId]
  def weights: Array[Distance]

}

trait DistanceVertex {

  def distance: Distance

}

trait BucketVertex {

  def isInto(bucket: Bucket): Boolean

}

case class TraitedVertex(
                          phaseDistance: Distance,
                          center: NodeId,
                          updated: Boolean,
                          offsetDistance: Distance,
                          override val neighbourhood: Array[NodeId],
                          override val weights: Array[Distance])
  extends WeightedVertex with DistanceVertex with BucketVertex {

  override val distance: Distance = phaseDistance + offsetDistance

  override def isInto(bucket: Bucket): Boolean =
    bucket.minDistance <= phaseDistance && phaseDistance < bucket.maxDistance

}

object TraitedVertex {

  def apply(edges: Iterable[(NodeId, Distance)]): TraitedVertex = {
    val _edges = edges.toSeq.sortBy(_._1)
    new TraitedVertex(
      phaseDistance = Infinity,
      center = -1,
      updated = false,
      offsetDistance = 0,
      neighbourhood = _edges.map(_._1).toArray,
      weights = _edges.map(_._2).toArray)
  }

}

case class MonolithicVertex(
                             phaseDistance: Distance,
                             center: NodeId,
                             updated: Boolean,
                             offsetDistance: Distance,
                             neighbourhood: Array[NodeId],
                             weights: Array[Distance]) {

  val distance = phaseDistance + offsetDistance

  def isInto(bucket: Bucket): Boolean =
    bucket.minDistance <= phaseDistance && phaseDistance < bucket.maxDistance

}

object MonolithicVertex {

  def apply(edges: Iterable[(NodeId, Distance)]): MonolithicVertex = {
    val _edges = edges.toSeq.sortBy(_._1)
    new MonolithicVertex(
      phaseDistance = Infinity,
      center = -1,
      updated = false,
      offsetDistance = 0,
      neighbourhood = _edges.map(_._1).toArray,
      weights = _edges.map(_._2).toArray)
  }

}