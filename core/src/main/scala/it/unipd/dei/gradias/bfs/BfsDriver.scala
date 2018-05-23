package it.unipd.dei.gradias.bfs

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import org.apache.spark.SparkContext

class BfsDriver (
                  override val sc: SparkContext,
                  override val input: String)
  extends AlgorithmDriver with MainTable with InputPath with DiameterInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "bfs"

  override def mainTable: Map[String, Any] = Map(
    "diameter" -> diameter,
    "t_diameter" -> Timer.getMillis("bfs"))

  override protected def computeDiameter(): Distance = {
    val raw = action("raw graph read") {
      UnweightedGraphLoader.loadGraph(sc, input).persist(Bfs.persistLevel)
    }
    Bfs(experiment)(raw)
  }
}
