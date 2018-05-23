package it.unipd.dei.gradias.diameter

import java.io.File

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.timed
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.diameter.dijkstra.Dijkstra
import it.unipd.dei.gradias.io.Inputs
import it.unipd.dei.gradias.util.ExperimentUtil.jMap
import it.unipd.dei.gradias.util.Logger._
import org.rogach.scallop.ScallopConf

import scala.collection.mutable
import scala.util.Random

object Estimator {

  private def farthest(distances: Array[Distance]): (NodeId, Distance, Boolean) = {
    var vVal: Distance = 0
    var vIdx = 0
    var it = 0
    val n = distances.length
    var connected = true
    while(it < n) {
      if(distances(it) < Infinity) {
        if (distances(it) > vVal) {
          vVal = distances(it)
          vIdx = it
        }
      } else {
        connected = false
      }
      it += 1
    }
    (vIdx, vVal, connected)
  }

  def estimate(graph: CSCLocalGraph): (Distance, NodeId, NodeId, Boolean) = {
    var bestDist: Distance = 0
    var src = Random.nextInt(graph.n)
    var dest = -1
    var connected = true
    var it = 0
    val visited = mutable.Set[Int]()

    timed("diameter estimation") {
      while (!visited.contains(src)) timed("sssp iteration") {
        algoLogger.info(s"Tentative $it, best guess ${Distance.toString(bestDist)}")
        it += 1
        visited.add(src)
        val distances = Dijkstra.sssp(src, graph)
        val (far, dist, conn) = farthest(distances)
        // If in a disconnected node, jump to another source
        dest = if (dist>0) far else Random.nextInt(graph.n)
        connected = connected && conn
        algoLogger.info(
          s"Found distance ${Distance.toString(dist)}, $src -> $far")
        if (dist > bestDist) {
          it = 0
          bestDist = dist
          src = far
        }
      }
    }

    (bestDist, src, dest, connected)
  }

  def main(args: Array[String]) {
    val experiment = new Experiment()
    val opts = new Options(args)
    experiment.tag("graph", new File(opts.input()).getName)

    val graph = CSCLocalGraph.fromSequenceFile(opts.input())

    val graphMetadata = Inputs.metadata(opts.input())
    for ((k, v) <- graphMetadata.toMap) {
      experiment.tag(k, v)
    }

    val (diameter, from, to, connected) = estimate(graph)

    algoLogger.info(s"The estimated diameter is ${Distance.toString(diameter)} ($from -> $to). Connected? $connected")

    experiment.append("statistics", jMap(
        graph.stats
          .updated("diameter", diameter)
          .updated("time", Timer.getMillis("diameter estimation")).toSeq : _*))

    graphMetadata.set("diameter", diameter.toString)
    graphMetadata.save()

    println(experiment.toSimpleString)
    experiment.saveAsJsonFile()
  }

  class Options(args: Array[String]) extends ScallopConf(args) {
    val input = opt[String](required = true)
  }

}
