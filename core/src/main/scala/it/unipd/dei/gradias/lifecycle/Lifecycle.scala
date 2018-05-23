package it.unipd.dei.gradias.lifecycle

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.timed
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.io.Inputs
import it.unipd.dei.gradias.util.ExperimentUtil._
import it.unipd.dei.gradias.util._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.ManifestInfo._
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Lifecycle structure
 */
trait Lifecycle {

  type T

  val experiment: Experiment

  def setup(): Unit = { }

  def teardown(): Unit = { }

  def tagExperiment(): Unit = { }

  def loadInput(): T

  def run(input: T): Unit

  def appendTables(): Unit = { }

  def updateMainTable(table: Map[String, Any]): Map[String, Any] = table

  def report(): Unit = {
    algoLogger.info("\n{}", experiment.toSimpleString)
    experiment.saveAsJsonFile("reports", false)
  }

  def execute(): Unit = {
    setup()
    tagExperiment()
    val input = loadInput()
    run(input)
    appendTables()
    val mainTable = updateMainTable(Map())
    experiment.append("main", jMap(mainTable.toArray :_*))
    report()
    teardown()
  }

}

trait WeightedInput extends Lifecycle with SparkInfo {

  type T = RDD[(EdgeId, Distance)]

  val input: String

  // this value can be used to compute TEPS
  val originalEdges: Accumulator[Long] = sc.accumulator(0L)

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment.tag("input", input)
  }

  def loadInput(): RDD[(EdgeId, Distance)] =
    timed("Input loading") {
      val raw = WeightedGraphLoader.load(sc, input, originalEdges)
      val metadata = Inputs.metadata(input)
      val graph = if(metadata.getOrElse("edges.canonical-orientation", "true").toBoolean) {
        algoLogger.info("Graph has edges in canonical orientation, symmetrizing it.")
        symmetrize(raw)
      } else {
        raw
      }
      graph
    }

}

trait BasicInfo extends Lifecycle {

  val algorithmName: String

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment.tag("algorithm", algorithmName)
  }

}

trait GradiasInfo extends Lifecycle {

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

trait SparkInfo extends Lifecycle {

  def sc: SparkContext

  val storageLevel = Utils.storageLevel

  val metrics = new GlobalMetrics()

  override def setup(): Unit = {
    super.setup()
    sc.addSparkListener(metrics)
    sc.addSparkListener(new ProgressLoggerListener())
    sc.addSparkListener(new ProfilingListener())
    sc.addSparkListener(new JobKillerListener(sc))
  }

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("spark-master", sc.master)
      .tag("spark-user", sc.sparkUser)
      .tag("spark-version", sc.version)
      .tag("spark-parallelism", sc.defaultParallelism)
  }

  override def appendTables(): Unit = {
    super.appendTables()
    experiment.append("environment",
      jMap(sc.getConf.getAll :_*))

    for((stageId, stageMetrics) <- metrics.stages.toSeq.sortBy(_._1)) {
      experiment.append("stage-metrics",
        jMap(stageMetrics.toMap.updated("stage", stageId).toArray :_*))
    }
  }

  override def updateMainTable(table: Map[String, Any]): Map[String, Any] =
    super.updateMainTable(table) ++ metrics.aggregate.toMap

  override def teardown(): Unit = {
    algoLogger.info("Stopping Spark context")
    sc.stop()
  }

}
