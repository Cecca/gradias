package it.unipd.dei.gradias.lifecycle

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.io.Inputs
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import it.unipd.dei.gradias.weighted.sssp.DeltaSteppingSSSP
import it.unipd.dei.gradias._
import org.apache.hadoop.fs.Path
import org.apache.spark.{HashPartitioner, SparkContext}
import Timer.timed

class DeltaSSSPLifecycle(
                          override val sc: SparkContext,
                          val input: String,
                          val source: Option[NodeId],
                          val delta: Distance,
                          val graphImpl: String)
extends Lifecycle with SparkInfo with BasicInfo with GradiasInfo {

  override type T = Graph[DeltaSteppingSSSP.Vertex]

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "delta-stepping-sssp"

  private var weightedRadius: Distance = -1.0f
  private var diameter: Distance = -1.0f
  private var originalEdges: Long = 0
  private var buckets = 0

  override def loadInput(): Graph[DeltaSteppingSSSP.Vertex] =
    timed("Input loading"){
      DeltaSSSPBlockGraphCache.load(sc, input)
    }

  override def run(input: Graph[DeltaSteppingSSSP.Vertex]): Unit = timed("time") {
    originalEdges = input.numEdges
    val (wr, nBuckets) = DeltaSteppingSSSP(delta, experiment).run(input, source)
    weightedRadius = wr
    diameter = 2*weightedRadius
    buckets = 0
  }

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("input", input)
      .tag("implementation", graphImpl)
      .tag("delta", delta)
      .tag("source", source)
      .tag("storageLevel", storageLevel.description)
  }

  override def updateMainTable(table: Map[String, Any]): Map[String, Any] = {
    val time = Timer.getMillis("delta-sssp")
    super.updateMainTable(table) ++ Map(
      "diameter" -> diameter,
      "weightedRadius" -> weightedRadius,
      "TEPS" -> originalEdges / time,
      "t_diameter" -> time,
      "time" -> time,
      "buckets" -> buckets
    )
  }


}

object DeltaSSSPBlockGraphCache {

  val cacheDir = "/tmp/spark-cache/delta-sssp"

  def graphPaths(name: String, numPartitions: Int) = {
    val baseName = new Path(name).getName
    val base = new Path(cacheDir, new Path(baseName, numPartitions.toString))
    val attributes = new Path(base, "attributes")
    val adjacencies = new Path(base, "adjacencies")
    (attributes, adjacencies)
  }

  def load(sc: SparkContext, input: String): Graph[DeltaSteppingSSSP.Vertex] = {
    val (attributes, adjacencies) = graphPaths(input, sc.defaultParallelism)
    val fs = attributes.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(attributes) && fs.exists(adjacencies)) {
      loadFromCache(sc, attributes, adjacencies)
    } else {
      val g = loadFromDisk(sc, input, DeltaSteppingSSSP.Vertex.apply)
      storeInCache(g.asInstanceOf[BlockGraph[DeltaSteppingSSSP.Vertex]], attributes, adjacencies)
      g
    }
  }

  def loadFromDisk(sc: SparkContext, input: String, initVertex: () => DeltaSteppingSSSP.Vertex): Graph[DeltaSteppingSSSP.Vertex] = {
    algoLogger.info("Loading graph from disk")
    val raw = WeightedGraphLoader.load(sc, input, sc.accumulator(0L))
    val metadata = Inputs.metadata(input)
    val graph = if (metadata.getOrElse("edges.canonical-orientation", "true").toBoolean) {
      algoLogger.info("Graph has edges in canonical orientation, symmetrizing it.")
      symmetrize(raw)
    } else {
      raw
    }
    BlockGraph.fromEdges(graph, initVertex)
      .setName("clean input")
      .persist(Utils.storageLevel)
  }

  def loadFromCache(sc: SparkContext, attributes: Path, adjacencies: Path): Graph[DeltaSteppingSSSP.Vertex] = {
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    algoLogger.info(s"Loading graph from cache! ($attributes || $adjacencies})")
    val attrs = sc.objectFile[(Int, NodeAttributesBlock[DeltaSteppingSSSP.Vertex])](attributes.toString, sc.defaultParallelism)
      .partitionBy(partitioner)
    val adjs = sc.objectFile[(Int, AdjacencyBlock)](adjacencies.toString, sc.defaultParallelism)
      .partitionBy(partitioner)
    val g = new BlockGraph[DeltaSteppingSSSP.Vertex](attrs, adjs)
      .setName("cached input")
      .persist(Utils.storageLevel)
    g.numEdges
    g.numNodes
    g
  }

  def storeInCache(graph: BlockGraph[DeltaSteppingSSSP.Vertex], attributes: Path, adjacencies: Path): Unit = {
    algoLogger.info(s"Storing graph in cache ($attributes || $adjacencies})")
    graph.attributes.saveAsObjectFile(attributes.toString)
    graph.adjacencies.saveAsObjectFile(adjacencies.toString)
  }

}
