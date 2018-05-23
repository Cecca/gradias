package it.unipd.dei.gradias.weighted

import it.unipd.dei.gradias.{Distance, EdgeId}
import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias.weighted.SymmetryUtils._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

object Shortcutter {

  /**
   * Add shortcuts to the given graph
   *
   * @param steps the maximum number of steps to perform
   * @param delta edges longer than `delta` will not be considered
   * @param graph the graph to apply shortcuts to
   * @return a shortcutted graph
   */
  def addShortcuts(steps: Int, delta: Distance, graph: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] = action("shortcutting") {

    val _g = initializeGraph(delta, graph)
    val shortcuts = action("shortcut computation") {
      computeShortcuts(steps, delta, _g)
    }
    _g.unpersist(blocking = false)
    finalizeGraph(_g, shortcuts)
  }

  def initializeGraph(delta: Distance, graph: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] = action("shortcutting initialization") {
    graph.desymmetrize().filter { case ((u, v), w) =>
      w < delta
    }
  }

  def finalizeGraph(original: RDD[(EdgeId, Distance)], shortcutted: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] = {

    val finalized = action("finalization") {
      original.union(shortcutted)
        .reduceByKey(math.min)
        .symmetrize()
        .cache()
    }
    original.unpersist(blocking = false)
    shortcutted.unpersist(blocking = false)

    finalized
  }

  @tailrec
  private def computeShortcuts(steps: Int, delta: Distance, graph: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] = {

    if(steps == 0)
      return graph

    val updatedGraph = action(s"shortcut computation ($steps steps left)") {
      val newEdges = graph.map { case ((u, v), w) =>
        (u, (v, w))
      }.groupByKey().flatMap { case (id, neighs) =>
        neighs.toSeq.combinations(2).map { case Seq((v1, w1), (v2, w2)) =>
          val key =
            if (v1 < v2)
              (v1, v2)
            else
              (v2, v1)
          (key, w1 + w2)
        }.filter(_._2 < delta)
      }

      graph.union(newEdges)
        .reduceByKey(math.min)
        .cache()
    }

    graph.unpersist(blocking = false)

    computeShortcuts(steps -1, delta, updatedGraph)
  }
}
