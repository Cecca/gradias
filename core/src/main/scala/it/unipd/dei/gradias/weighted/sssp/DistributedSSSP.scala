package it.unipd.dei.gradias.weighted.sssp

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.GraphImplicits._
import it.unipd.dei.gradias.Timer._
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.{Utils, ExperimentUtil}
import it.unipd.dei.gradias.util.Logger._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

object DistributedSSSP {

  val persistLevel = Utils.storageLevel

  def apply(experiment: Experiment): DistributedSSSP =
    new DistributedSSSP(experiment)

  def initGraph(raw: RDD[(EdgeId, Distance)], implementation: String)
  : Graph[Vertex] = {
    Graph.fromEdges(raw, Vertex.apply, implementation)
  }

  case class Vertex(
                     distance: Distance,
                     updated: Boolean)
    extends UpdatableVertex with DistanceInfo

  object Vertex {

    def apply(): Vertex =
      new Vertex(Infinity, false)

  }

}

class DistributedSSSP(@transient experiment: Experiment) extends Serializable {
  import DistributedSSSP._

  def run(raw: RDD[(EdgeId, Distance)], graphImpl: String): (Distance, Int) = {
    val graph = action("Initialization") {
      initGraph(raw, graphImpl).setName("Graph").persist(persistLevel)
    }
    run(graph)
  }

  def run(graph: Graph[Vertex]): (Distance, Int) = action("sssp") {

    val marked = action("source marking") {
      val source = graph.nodes.takeSample(false, 1)(0)._1
      algoLogger.info(s"Selected $source as the source")
      experiment.tag("source", source)
      graph.mapNodes ({ case (id, v) =>
        if (id == source)
          v.copy(distance = 0, updated = true)
        else
          v
      }).setName("marked graph").persist(persistLevel)
    }

    graph.unpersistNodes(blocking = false)

    val (result, hopRadius) = doExpansion(marked)

    val weightedRadius = result.nodes.map(_._2.distance).reduce(math.max)

    (weightedRadius, hopRadius)
  }

  @tailrec
  final def doExpansion(graph: Graph[Vertex], it: Int = 0)
  : (Graph[Vertex], Int) = {

    val msgAcc = graph.sparkContext.accumulator(0L)
    val (updatedGraph, updatedNodes) = action(s"growing step $it") {
      val _g = graph.exchangeMessages[Vertex, Distance](
        shouldSend = { (id, v) => v.updated },
        shouldTransmit = { (src, edge, w) => true },
        makeMessage = { (id, v, dest, weight) => Distance.sum(v.distance, weight) },
        mergeMessages = math.min,
        joinFunc = {
          case (v, Some(dist)) if dist < v.distance =>
            v.copy(distance = dist, updated = true)
          case (v, _) => v.copy(updated = false)
        },
        msgAcc
      ).setName(s"graph iteration $it").persist(persistLevel)

      _g.checkpointEvery(Utils.checkpointInterval)
      val uNodes = _g.countMatching((_, v) => v.updated)
      (_g, uNodes)
    }

    experiment.append("iterations",
      ExperimentUtil.jMap(
        "iteration" -> it,
        "messages" -> msgAcc.value,
        "updatedNodes" -> updatedNodes,
        "time" -> Timer.getMillis(s"growing step $it")))

    algoLogger.info(s"Updated ${yellow(updatedNodes)} nodes")

    if (updatedNodes > 0) {
      graph.unpersistNodes(blocking = false)
      doExpansion(updatedGraph, it + 1)
    } else {
      (graph, it - 1)
    }
  }

  def joinMessages(pair: (Vertex, Option[Distance])): Vertex = pair match {
    case (v, Some(dist)) if dist < v.distance =>
      v.copy(distance = dist, updated = true)
    case (v, _) => v.copy(updated = false)
  }

}
