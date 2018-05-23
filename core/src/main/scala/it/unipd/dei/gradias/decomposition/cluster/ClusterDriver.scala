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

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.diameter.WeightedDiameter.Algorithm
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ClusterDriver (
                      override val sc: SparkContext,
                      override val input: String,
                      val targetFraction: Double,
                      override val skipDiameter: Boolean = false,
                      override val diameterAlgorithm: Algorithm = "Distributed")
  extends AlgorithmDriver with MainTable with InputPath with DiameterFromQuotient[ClusterVertex] {

  override val experiment: Experiment = new Experiment()

  val algorithmName: String = "cluster"

  override def computeNodes(): RDD[(NodeId, ClusterVertex)] = {
    val inputGraph = action("Input loading") {
      UnweightedGraphLoader.loadGraph(sc, input).setName("Input graph")
    }
    ClusterDecomposition.decomposition(
      inputGraph,
      targetFraction)
  }

  override def computeEdges(): RDD[(EdgeId, Distance)] =
    new EdgeBuilder(ClusterDecomposition.persistLevel).buildEdges(nodes)

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("targetFraction", targetFraction)
  }

  def mainTable: Map[String, Any] = Map(
    "diameter" -> diameter,
    "radius" -> radius,
    "t_decomposition" -> (Timer.getMillis("decomposition") + Timer.getMillis("Edge construction")),
    "t_clustering" -> Timer.getMillis("decomposition"),
    "t_conversion" -> Timer.getMillis("Edge construction"),
    "nodes" -> quotientNodes,
    "edges" -> quotientEdges)

}