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

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.diameter.WeightedDiameter.Algorithm
import it.unipd.dei.gradias.diameter.{DiameterMatrices, WeightedDiameter}
import it.unipd.dei.gradias.util.ExperimentUtil.jMap
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.ManifestInfo.manifestMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

trait AlgorithmDriver extends ExperimentInfo with BasicInfo with GradiasInfo with SparkInfo {

  def apply(): Unit = {
    tagExperiment()
    appendTable()
    report()
  }

}

// TODO: Force implementors to declare an experiment as failed or not
trait ExperimentInfo {

  val experiment: Experiment

  def tagExperiment(): Unit = { }

  def appendTable(): Unit = { }

  def report(): Unit = {
    algoLogger.info("\n{}", experiment.toSimpleString)
    experiment.saveAsJsonFile("reports", true)
  }

  def fail(reason: String): Unit = {
    experiment.failed()
    experiment.note("Failure reason:" + reason)
    report()
  }

}

trait BasicInfo extends ExperimentInfo {

  val algorithmName: String

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment.tag("algorithm", algorithmName)
  }
}

trait GradiasInfo extends ExperimentInfo {

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    manifestMap.foreach { m =>
      import java.util.jar.Attributes.Name._

      import it.unipd.dei.gradias.util.ManifestKeys._
      experiment
        .tag("version", m.getOrElse(IMPLEMENTATION_VERSION, "-"))
        .tag("git branch", m.getOrElse(GIT_BRANCH, "-"))
        .tag("git build date", m.getOrElse(GIT_BUILD_DATE, "-"))
        .tag("git head rev", m.getOrElse(GIT_HEAD_REV, "-"))
    }
  }
}

trait InputPath extends ExperimentInfo {

  val input: String

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment.tag("input", input)
  }
}

trait SparkInfo extends ExperimentInfo {

  def sc: SparkContext

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("spark-master", sc.master)
      .tag("spark-user", sc.sparkUser)
      .tag("spark-version", sc.version)
      .tag("spark-parallelism", sc.defaultParallelism)
  }

  override def appendTable(): Unit = {
    super.appendTable()
    experiment.append("environment",
      jMap(sc.getConf.getAll :_*))
  }
}

trait MainTable extends ExperimentInfo {

  def mainTable: Map[String, Any]

  override def appendTable(): Unit = {
    super.appendTable()
    experiment.append("main",
      jMap(mainTable.toArray :_*))

  }
}

trait EdgesInfo extends ExperimentInfo {

  def computeEdges(): RDD[(EdgeId, Distance)]

  lazy val edges: RDD[(EdgeId, Distance)] = try {
    computeEdges()
  } catch {
    case e: Exception =>
      fail(e.getMessage)
      throw e
  }

  lazy val quotientEdges: Long = edges.count()

  def weightsDistribution: Array[(Distance, Int)] = edges.map {
    case (id, w) => (w, 1)
  }.reduceByKey(_ + _)
    .collect().sortBy(_._1)

  override def appendTable(): Unit = {
    super.appendTable()
    for((r, cnt) <- weightsDistribution) {
      experiment.append("weights",
        jMap(
          "weight" -> r,
          "count" -> cnt))
    }
  }
}

trait NodesInfo[V <: CenterInfo with DistanceInfo] extends ExperimentInfo {

  def computeNodes(): RDD[(NodeId, V)]

  lazy val nodes: RDD[(NodeId, V)] = try {
    computeNodes()
  } catch {
    case e: Exception =>
      fail(e.getMessage)
      throw e
  }

  lazy val quotientNodes: Long = CenterInfo.countCenters(nodes)

  lazy val radius: Distance = radiusDistribution.map(_._1).max

  def radiusDistribution: Array[(Distance, Int)] = nodes.map {
    case (id, v) => (v.center, v.distance)
  }.reduceByKey(math.max).map {
    case (_, r) => (r, 1)
  }.reduceByKey(_ + _)
    .collect().sortBy(_._1)

  override def appendTable(): Unit = {
    super.appendTable()
    for((r, cnt) <- radiusDistribution) {
      experiment.append("radius",
        jMap(
          "radius" -> r,
          "count" -> cnt))
    }
  }
}

trait DiameterInfo extends ExperimentInfo {

  protected def computeDiameter(): Distance

  val skipDiameter: Boolean = false

  def diameter: Distance = {
    if(skipDiameter)
      -1
    else {
      try {
        computeDiameter()
      } catch {
        case e: Exception =>
          fail(e.getMessage)
          throw e
      }
    }
  }

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("skip-diameter", skipDiameter)
  }
}

trait DiameterFromQuotient[V <: CenterInfo with DistanceInfo]
  extends DiameterInfo with NodesInfo[V] with EdgesInfo with SparkInfo {

  private def diameterMatrices: DiameterMatrices =
    DiameterMatrices.leanGraphToMatrices[V](nodes, edges)

  val diameterAlgorithm: Algorithm = "Distributed"

  override def computeDiameter(): Distance =
    new WeightedDiameter(sc, diameterMatrices, diameterAlgorithm).diameter

}