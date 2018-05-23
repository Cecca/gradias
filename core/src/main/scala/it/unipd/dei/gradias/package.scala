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

package it.unipd.dei

import org.apache.spark.rdd.RDD


package object gradias {

  type NodeId = Int
  type EdgeId = (NodeId, NodeId)

  type Distance = Float
  val Infinity: Distance = Float.PositiveInfinity

  object Distance {

    def sum(a: Distance, b: Distance): Distance = {
      val res = a + b
      if (res < 0)
        Infinity
      else
        res
    }

    def toString(d: Distance): String =
      if (d == Infinity)
        "∞"
      else
        d.toString

  }

  /**
   * This trait provides information about the center that captured
   * the vertex.
   */
  trait CenterInfo {

    /**
     * The ID of the node that captured the vertex. Note that this may be the
     * vertex itself.
     * @return the ID of the capturing vertex
     */
    def center: NodeId

    /**
     * Tells whether the vertex is a center
     * @return true if the vertex is a center
     */
    def isCenter: Boolean

  }

  object CenterInfo {

    def countCenters[V <: CenterInfo](graph: RDD[(NodeId, V)]): Long =
      graph.filter{ case (_, v) => v.isCenter }.count()

  }

  /**
   * This trait provides information about the distance between the vertex and
   * its capturing center
   */
  trait DistanceInfo {

    def distance: Distance

  }

  /**
   * Provides information about the neighbourhood of the vertex
   */
  trait NeighbourhoodInfo {

    def neighbourhood: Array[NodeId]

  }

  /**
   * Trait that allows a vertex to be marked as updated.
   */
  trait UpdatableVertex {

    def updated: Boolean

  }

  object UpdatableVertex {

    def countUpdated[V <: UpdatableVertex](graph: RDD[(NodeId, V)]): Long =
      graph.filter{ case (_, v) => v.updated }.count()


  }

  trait CoveringInfo {

    def isCovered: Boolean = !isUncovered

    def isUncovered: Boolean

  }

  object CoveringInfo {

    def countUncovered[V <: CoveringInfo](graph: RDD[(NodeId, V)]): Long =
      graph.filter{ case (_, v) => v.isUncovered }.count()

  }

  trait QuotientInfo {

    /**
     * Tells wether the vertex should be counted as a quotient vertex
     */
    def isQuotient: Boolean

  }

  object QuotientInfo {

    def countQuotient[V <: QuotientInfo](graph: RDD[(NodeId, V)]): Long =
      graph.filter{ case (_, v) => v.isQuotient }.count()

  }

}
