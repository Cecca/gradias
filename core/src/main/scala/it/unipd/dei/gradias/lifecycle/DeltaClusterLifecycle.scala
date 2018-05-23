package it.unipd.dei.gradias.lifecycle

import java.security.MessageDigest

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.timed
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.diameter.dijkstra.Dijkstra
import it.unipd.dei.gradias.diameter.{CSCLocalGraph, Estimator}
import it.unipd.dei.gradias.io.Inputs
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.util.{IntIdRemapping, Utils}
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import it.unipd.dei.gradias.weighted.decomposition.DeltaCluster.Vertex
import it.unipd.dei.gradias.weighted.decomposition.{DeltaCluster, RefinementStrategy}
import it.unipd.dei.gradias.weighted.heuristic.{FarthestCenterHeuristic, Heuristic}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.{Accumulator, HashPartitioner, SparkContext}

class DeltaClusterLifecycle(
                             override val sc: SparkContext,
                             val input: String,
                             target: Long,
                             delta: Distance,
                             refinementStrategy: RefinementStrategy,
                             steps: Int,
                             graphImpl: String,
                             diameterAlgorithm: String,
                             heuristicName: Option[String])
extends Lifecycle with SparkInfo with BasicInfo with GradiasInfo {

  override type T = Graph[Vertex]

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "delta-cluster"

  var originalEdges: Long = 0L

  val heuristic = heuristicName.map(getHeuristic)

  private var diameter: Distance = -1
  private var connected: Boolean = true

  private val remapping = new IntIdRemapping()
  private var edges: Seq[(NodeId, NodeId, Distance)] = _
  private var radiuses: Array[Distance] = _
  private var quotient: CSCLocalGraph = _

  private var numEdges = -1
  private var numNodes = -1
  private var radius: Distance = -1

  override def loadInput(): Graph[Vertex] =
    timed("Input loading") {
      DeltaClusterBlockGraphCache.load(sc, input)
    }

  override def run(input: Graph[Vertex]): Unit = timed("time") {
    originalEdges = input.numEdges
    val rawRes = {
      val (decompRes, finalDelta) = new DeltaCluster(experiment).decomposition(
        input,
        target,
        delta,
        refinementStrategy,
        steps,
        graphImpl)
      heuristic match {
        case None => decompRes
        case Some(h) => timed("heuristic") {
          algoLogger.info(s"Running heuristic $h")
          h.run(decompRes, finalDelta, experiment)
        }
      }
    }
    timed("conversion") {
      edges = timed("Edge construction") {
        new EdgeBuilder(Utils.storageLevel).buildEdges(rawRes).collect().flatMap { case ((src, dst), w) =>
          val u = remapping.update(src)
          val v = remapping.update(dst)
          Seq((u, v, w), (v, u, w))
        }
      }
      radiuses = rawRes.nodes.map{case (_, vinfo) =>
        (vinfo.center, vinfo.distance)
      }.reduceByKey(math.max).collect().map { case (c, r) =>
        (remapping(c), r)
      }.sorted.map(_._2)
      quotient = CSCLocalGraph.fromSmallIdEdges(edges.iterator)
    }
    numNodes = radiuses.length
    numEdges = edges.length
    radius = radiuses.max
    timed("diameter") {
      diameter = computeDiameter(quotient)
    }
  }

  def computeDiameter(graph: CSCLocalGraph): Distance = diameterAlgorithm match {
    case "Distributed" =>
      val distances = Dijkstra.apsp(sc, quotient)
      var i = 0
      var maxDistance = 0.0f
      while (i < distances.length) {
        var j = 0
        while (j < distances.length) {
          if (distances(i)(j) < Infinity) {
            maxDistance = math.max(
              maxDistance, distances(i)(j) + radiuses(i) + radiuses(j))
          } else {
            connected = false
          }
          j += 1
        }
        i += 1
      }
      maxDistance
    case "Dijkstra" =>
      algoLogger.info("Compute diameter with multiple Dijkstra passes")
      val (est, from, to, conn) = Estimator.estimate(graph)
      connected = conn
      est + radiuses(from) + radiuses(to)
  }

  override def tagExperiment(): Unit = {
    super.tagExperiment()
    experiment
      .tag("input", input)
      .tag("implementation", graphImpl)
      .tag("refinement", refinementStrategy.toString)
      .tag("target", target)
      .tag("heuristic", heuristicName)
      .tag("initial-delta", delta)
      .tag("storageLevel", storageLevel.description)
      .tag("diameterAlgorithm", diameterAlgorithm)
  }

  override def updateMainTable(table: Map[String, Any]): Map[String, Any] = {
    val time = Seq("clustering", "heuristic", "conversion", "diameter").map(Timer.getMillis).sum
    super.updateMainTable(table) ++ Map(
      "diameter" -> diameter,
      "radius" -> radius,
      "TEPS" -> originalEdges / time,
      "TEPS-clustering" -> originalEdges / Timer.getMillis("clustering"),
      "connected" -> connected,
      "t_clustering" -> Timer.getMillis("clustering"),
      "t_heuristic" -> Timer.getMillis("heuristic"),
      "t_conversion" -> Timer.getMillis("conversion"),
      "t_diameter" -> Timer.getMillis("diameter"),
      "time" -> time,
      "nodes" -> numNodes,
      "edges" -> numEdges
    )
  }

  def getHeuristic(name: String): Heuristic = name match {
    case "farthest" => new FarthestCenterHeuristic()
    case _ => throw new IllegalArgumentException(s"unsupported heuristic $name")
  }
}

object DeltaClusterBlockGraphCache {

  val cacheDir = "/tmp/spark-cache/delta-cluster"

  def graphPaths(name: String, numPartitions: Int, hadoopConf: Configuration) = {
    val path = new Path(name)
    val fs = path.getFileSystem(hadoopConf)
    val urlPath = fs.getFileStatus(path).getPath.toUri.toString
    val baseName = DigestUtils.sha256Hex(urlPath)
    algoLogger.info(s"SHA-256 of $urlPath = $baseName")
    val base = new Path(cacheDir, new Path(baseName, numPartitions.toString))
    val attributes = new Path(base, "attributes")
    val adjacencies = new Path(base, "adjacencies")
    (attributes, adjacencies)
  }

  def load(sc: SparkContext, input: String): Graph[Vertex] = {
    val (attributes, adjacencies) = graphPaths(input, sc.defaultParallelism, sc.hadoopConfiguration)
    val fs = attributes.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(attributes) && fs.exists(adjacencies)) {
      loadFromCache(sc, attributes, adjacencies)
    } else {
      val g = loadFromDisk(sc, input, Vertex.apply)
      storeInCache(g.asInstanceOf[BlockGraph[Vertex]], attributes, adjacencies)
      g
    }
  }

  def loadFromDisk(sc: SparkContext, input: String, initVertex: () => Vertex): Graph[Vertex] = {
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

  def loadFromCache(sc: SparkContext, attributes: Path, adjacencies: Path): Graph[Vertex] = {
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    algoLogger.info(s"Loading graph from cache! ($attributes || $adjacencies})")
    val attrs = sc.objectFile[(Int, NodeAttributesBlock[Vertex])](attributes.toString, sc.defaultParallelism)
      .partitionBy(partitioner)
    val adjs = sc.objectFile[(Int, AdjacencyBlock)](adjacencies.toString, sc.defaultParallelism)
      .partitionBy(partitioner)
    val g = new BlockGraph[Vertex](attrs, adjs)
      .setName("cached input")
      .persist(Utils.storageLevel)
    g.numEdges
    g.numNodes
    g
  }

  def storeInCache(graph: BlockGraph[Vertex], attributes: Path, adjacencies: Path): Unit = {
    algoLogger.info(s"Storing graph in cache ($attributes || $adjacencies})")
    graph.attributes.saveAsObjectFile(attributes.toString)
    graph.adjacencies.saveAsObjectFile(adjacencies.toString)
  }

}
