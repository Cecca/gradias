package it.unipd.dei.gradias.lifecycle

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.weighted.sssp.DistributedSSSP
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DistributedSSSPLifeclycle(
                                 override val sc: SparkContext,
                                 override val input: String,
                                 graphImpl: String)
extends Lifecycle with WeightedInput with BasicInfo with GradiasInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "sssp"

  private var weightedRadius: Distance = -1
  private var hopRadius = -1

  override def run(input: RDD[((NodeId, NodeId), Distance)]): Unit = {
    val (wRadius, hRadius) = DistributedSSSP(experiment).run(input, graphImpl)
    weightedRadius = wRadius
    hopRadius = hRadius
  }


  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment.tag("implementation", graphImpl)
  }

  override def updateMainTable(table: Map[String, Any]): Map[String, Any] =
    super.updateMainTable(table) ++ Map(
      "diameter" -> weightedRadius * 2,
      "weighted radius" -> weightedRadius,
      "hop radius" -> hopRadius,
      "time" -> Timer.getMillis("sssp")
    )
}
