package it.unipd.dei.gradias.weighted.sssp

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import org.apache.spark.SparkContext

class DistributedSSSPDriver (
                              override val sc: SparkContext,
                              override val input: String)
  extends AlgorithmDriver with MainTable with InputPath with DiameterInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "sssp"

  override def mainTable: Map[String, Any] = Map(
    "diameter" -> diameter,
    "weighted radius" -> weightedRadius,
    "hop radius" -> hopRadius,
    "t_diameter" -> Timer.getMillis("sssp"))

  lazy val (weightedRadius, hopRadius) = {
    val raw = action("raw graph read") {
      WeightedGraphLoader.load(sc, input, sc.accumulator(0L), DistributedSSSP.persistLevel)
        .symmetrize().persist(DistributedSSSP.persistLevel)
    }
    DistributedSSSP(experiment).run(raw, "blocks")
  }

  override protected def computeDiameter(): Distance = weightedRadius * 2

}
