package it.unipd.dei.gradias.weighted.sssp

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import org.apache.spark.SparkContext
import it.unipd.dei.gradias.weighted.SymmetryUtils._

class DeltaSteppingSSSPDriver (
                                override val sc: SparkContext,
                                val delta: Distance,
                                val source: Option[NodeId],
                                override val input: String)
  extends AlgorithmDriver with MainTable with InputPath with DiameterInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "delta-sssp"

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("delta", delta)
  }

  override def mainTable: Map[String, Any] = Map(
    "diameter" -> diameter,
    "weighted radius" -> weightedRadius,
    "buckets" -> buckets,
    "t_diameter" -> Timer.getMillis("delta-sssp"))

  lazy val (weightedRadius, buckets) = {
    val raw = action("raw graph read") {
      WeightedGraphLoader.load(sc, input, sc.accumulator(0L), DeltaSteppingSSSP.persistLevel)
        .symmetrize().persist(DeltaSteppingSSSP.persistLevel)
    }
    DeltaSteppingSSSP(delta, experiment).run(raw, source)
  }

  override protected def computeDiameter(): Distance = weightedRadius * 2

}
