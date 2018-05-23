package it.unipd.dei.gradias.anf

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import org.apache.spark.SparkContext

class HadiDriver(
                  override val sc: SparkContext,
                  override val input: String,
                  val numCounters: Int)
  extends AlgorithmDriver with MainTable with InputPath with DiameterInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "hadi"

  override def mainTable: Map[String, Any] = Map(
    "diameter" -> diameter,
    "t_diameter" -> Timer.getMillis("hadi"))

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("counters", numCounters)
  }

  override protected def computeDiameter(): Distance = {
    val raw = action("raw graph read") {
      UnweightedGraphLoader.loadGraph(sc, input)
        .setName("raw input").persist(Hadi.persistLevel)
    }
    action("hadi") {
      Hadi(experiment).run(raw, numCounters)
    }
  }
}
