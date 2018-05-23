package it.unipd.dei.gradias.weighted.sssp

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.ExperimentUtil._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.{Bucket, BucketInfo}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import GraphImplicits._

import scala.annotation.tailrec

object DeltaSteppingSSSP {

  val persistLevel = Utils.storageLevel

  def apply(delta: Distance, experiment: Experiment): DeltaSteppingSSSP =
    new DeltaSteppingSSSP(delta, experiment)

  def initGraph(delta: Distance, raw: RDD[(EdgeId, Distance)])
  : Graph[Vertex] = {
    val g = EdgeGraph.fromEdges(raw, Vertex.apply)
      .setName("Initial graph")
      .persist(persistLevel)
    algoLogger.info(
      s"Initialized graph with ${g.numNodes} nodes and ${g.numEdges} edges")
    g
  }

  case class Vertex(
                     distance: Distance,
                     updated: Boolean)
    extends UpdatableVertex with DistanceInfo with BucketInfo with CoveringInfo {

    def isUncovered: Boolean = distance == Infinity

    def bucketDistance() = distance

  }

  object Vertex {

    def apply(): Vertex =
      new Vertex(
        distance = Infinity,
        updated = false)

  }

}

class DeltaSteppingSSSP(
                         val delta: Distance,
                         @transient experiment: Experiment) extends Serializable {
  import DeltaSteppingSSSP._

  private var globalRelaxationIndex: Int = 0

  def run(raw: RDD[(EdgeId, Distance)], source: Option[NodeId]): (Distance, Int) = {
    val graph = action("Initialization") {
      initGraph(delta, raw).setName("Graph").persist(persistLevel)
    }
    run(graph, source)
  }

  def run(graph: Graph[Vertex], sourceId: Option[NodeId]): (Distance, Int) = action("delta-sssp") {
    globalRelaxationIndex = 0

    val marked = action("source marking") {
      val source = sourceId match {
        case Some(src) =>
          require(graph.nodes.keys.filter(id => id == src).count == 1,
            s"The requested node $src is not in the graph.")
          src
        case None => graph.nodes.takeSample(false, 1)(0)._1
      }
      algoLogger.info(s"Using $source as the source")
      experiment.tag("source", source)
      graph.mapNodes ({ case (id, v) =>
        if (id == source)
          v.copy(distance = 0, updated = true)
        else
          v
      }).setName("marked graph").persist(persistLevel)
    }

    graph.unpersistNodes(blocking = false)

    val (result, buckets) = doExpansion(marked)

//    algoLogger.info(
//      s"Final bucket distribution ${new Bucket(delta, 0).bucketString(result.nodes)}")

    val weightedRadius = result.nodes.map(_._2.distance).filter(_ != Infinity).reduce(math.max)

    (weightedRadius, buckets)
  }

  @tailrec
  final def doExpansion(graph: Graph[Vertex], bucketIdx: Int = 0)
  : (Graph[Vertex], Int) = {

    val bucket = findNextBucket(graph, bucketIdx)

    algoLogger.info("%s Computing bucket %s %s".format(
      red("========"), bucket.index, red("========")))

    val updatedGraph = deltaStep(bucket, graph)

    val updatedCnt = updatedGraph.countUpdated()

    if (updatedCnt > 0) {
      graph.unpersistNodes(blocking = false)
      doExpansion(updatedGraph, bucket.index + 1)
    } else {
      (updatedGraph, bucket.index - 1)
    }
  }

  def findNextBucket(graph: Graph[Vertex], minIndex: Int): Bucket = {
    val idx = graph.nodes.values.map { vertex =>
      if (vertex.bucketDistance() == Infinity)
        Infinity.toInt
      else
        (vertex.bucketDistance() / delta).toInt // the bucket ID
    }.filter(_ >= minIndex).reduce(math.min)
    if(idx == Infinity.toInt) {
      throw new IllegalStateException(s"${Distance.toString(idx)} is not a valid index!")
    }
    new Bucket(delta, idx)
  }

  def deltaStep(bucket: Bucket, graph: Graph[Vertex])
  : Graph[Vertex] = action(s"Delta step for $bucket") {

    val (lightUpdated, numRelaxations) = lightRelaxations(bucket, graph, 0)
    val updatedGraph = heavyRelaxations(bucket, lightUpdated, numRelaxations+1)

    updatedGraph
  }

  @tailrec
  final def lightRelaxations(bucket: Bucket, graph: Graph[Vertex], relaxationIndex: Int)
  : (Graph[Vertex], Int) = {

    val start = System.currentTimeMillis()
    val msgAcc = graph.sparkContext.accumulator(0L)

    val updatedGraph = graph.exchangeMessages[Vertex, Distance](
      shouldSend = { (id, v) => v.updated && bucket.contains(v) },
      shouldTransmit = { (src, edge, w) => bucket.isLight(w) },
      makeMessage = { (id, v, dest, weight) => Distance.sum(v.distance, weight) },
      mergeMessages = math.min,
      joinFunc = {
        case (vertex, Some(dist)) if vertex.isUncovered || dist < vertex.distance =>
          vertex.copy(distance = dist, updated = true)
        case (vertex, _) if bucket.contains(vertex) => vertex.copy(updated = false)
        case (vertex, _) => vertex
      },
      msgAcc
    ).setName(s"light relaxation (bucket ${bucket.index})").persist(persistLevel)

    updatedGraph.checkpointEvery(Utils.checkpointInterval)

    val numUpdated = updatedGraph.countBucketUpdated(bucket)
    algoLogger.info(s"Light step updated ${yellow(numUpdated)} nodes in bucket ${bucket.index}")
    bucket.logBucketString(updatedGraph.nodes)

    graph.unpersistNodes()

    val end = System.currentTimeMillis()
    experiment.append("relaxations",
      jMap(
        "idx" -> globalRelaxationIndex,
        "bucket" -> bucket.index,
        "relaxationIndex" -> relaxationIndex,
        "type" -> "light",
        "updateRequests" -> msgAcc.value,
        "updatedNodes" -> numUpdated,
        "time" -> (end - start)))
    globalRelaxationIndex += 1

    if (numUpdated == 0) {
      (updatedGraph, relaxationIndex)
    } else {
      lightRelaxations(bucket, updatedGraph, relaxationIndex+1)
    }
  }

  def heavyRelaxations(bucket: Bucket, graph: Graph[Vertex], relaxationIndex: Int)
  : Graph[Vertex] = {

    val start = System.currentTimeMillis()
    val msgAcc = graph.sparkContext.accumulator(0L)

    val updatedGraph = graph.exchangeMessages[Vertex, Distance](
      shouldSend = { (id, v) => bucket.contains(v) },
      shouldTransmit = { (src, edge, w) => !bucket.isLight(w) },
      makeMessage = { (id, v, dest, weight) => Distance.sum(v.distance, weight) },
      mergeMessages = math.min,
      joinFunc = {
        case (vertex, Some(dist)) if vertex.isUncovered || dist < vertex.distance =>
          vertex.copy(distance = dist, updated = true)
        case (vertex, _) if bucket.contains(vertex) => vertex.copy(updated = false)
        case (vertex, _) => vertex
      },
      msgAcc
    )

    val numUpdated = updatedGraph.countUpdated()
    algoLogger.info(s"Heavy step updated ${yellow(numUpdated)} nodes")
    bucket.logBucketString(updatedGraph.nodes)

    graph.unpersistNodes()

    val end = System.currentTimeMillis()
    experiment.append("relaxations",
      jMap(
        "idx" -> globalRelaxationIndex,
        "bucket" -> bucket.index,
        "relaxationIndex" -> relaxationIndex,
        "type" -> "heavy",
        "updateRequests" -> msgAcc.value,
        "updatedNodes" -> numUpdated,
        "time" -> (end - start)))
    globalRelaxationIndex += 1

    updatedGraph
  }

}
