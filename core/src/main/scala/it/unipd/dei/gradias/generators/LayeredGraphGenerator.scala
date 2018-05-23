package it.unipd.dei.gradias.generators

import it.unipd.dei.gradias.{Distance, EdgeId}
import it.unipd.dei.gradias.io.{Inputs, EdgeKey}
import it.unipd.dei.gradias.util.Logger.algoLogger
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.WeightedGraphLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LayeredGraphGenerator(
                             val baseGraphName: String,
                             val layers: Int,
                             val interLayerWeight: Float)
extends Generator with Serializable {

  override def generate(): Iterator[(EdgeKey, Float)] = {
    val numNodes = LayeredGraphGenerator.sequentialCountNodes(baseGraphName)
    val baseGraphEdges = Inputs.sequenceFile(baseGraphName)
    val interLayer: Iterator[(EdgeKey, Float)] =
      (0L until numNodes).iterator.flatMap { v =>
        (0 until (layers-1)).map { l =>
          (new EdgeKey((l*numNodes) + v, ((l+1)*numNodes) + v), interLayerWeight)
        }
      }

    val edgeClones: Iterator[(EdgeKey, Float)] =
      baseGraphEdges.flatMap { case (edge, w) =>
        val u = edge.src
        val v = edge.dst
        (0 until layers).map { l =>
          (new EdgeKey((l*numNodes) + u, (l*numNodes) + v), w)
        }
      }

    interLayer ++ edgeClones
  }

  override def parallel(sc: SparkContext): RDD[(EdgeKey, Float)] = {
    LayeredGraphGenerator.checkRequirements(baseGraphName)
    val baseGraphEdges = WeightedGraphLoader.load(sc, baseGraphName, sc.accumulator(0L), Utils.storageLevel)
    val numNodes = baseGraphEdges.keys.map{case (u, v) => math.max(u, v)}.reduce(math.max) + 1

    val interLayerEdges = buildInterLayerEdges(sc, numNodes)
    val edgeClones = cloneEdges(baseGraphEdges, numNodes)

    interLayerEdges.union(edgeClones)
  }

  def cloneEdges(baseEdges: RDD[(EdgeId, Distance)], numNodes: Int): RDD[(EdgeKey, Float)] =
    baseEdges.flatMap { case ((u, v), w) =>
      (0 until layers).map { l =>
        (new EdgeKey((l*numNodes) + u, (l*numNodes) + v), w)
      }.asInstanceOf[TraversableOnce[(EdgeKey, Float)]]
    }

  def buildInterLayerEdges(sc: SparkContext, numNodes: Int): RDD[(EdgeKey, Float)] = {
    val nodesPerPartSeq = {
      if (sc.defaultParallelism > 1) {
        val numParts = sc.defaultParallelism - 1 // one less because the last will contain the remainder
        val nodesPerPart: Int = numNodes / numParts
        val nodesInLastPart: Int = numNodes % numParts
        val lastPartSeq = if (nodesInLastPart != 0) Seq((numNodes - nodesInLastPart, numNodes)) else Seq.empty
        require(numParts * nodesPerPart + nodesInLastPart == numNodes)
        (0 until numParts).foldLeft(Seq.empty[(Int, Int)]) { case (acc, idx) =>
          acc :+ ((idx * nodesPerPart, (idx + 1) * nodesPerPart))
        } ++ lastPartSeq
      } else {
        Seq((0, numNodes))
      }
    }
    algoLogger.info(s"$nodesPerPartSeq")
    sc.parallelize(nodesPerPartSeq).flatMap { case (start, end) =>
      (start until end).iterator.flatMap { v =>
        (0 until (layers-1)).map { l =>
          (new EdgeKey((l*numNodes) + v, ((l+1)*numNodes) + v), interLayerWeight)
        }
      }
    }
  }


}

object LayeredGraphGenerator {

  def sequentialCountNodes(fileName: String): Long = {
    checkRequirements(fileName)
    algoLogger.info("Counting nodes")
    var maxId = 0L
    for ((edge, _) <- Inputs.sequenceFile(fileName)) {
      maxId = math.max(maxId, edge.dst) // destination is bigger than source, because of canonical orientation
    }
    val numNodes = maxId + 1
    algoLogger.info(s"Counted $numNodes nodes")
    numNodes
  }

  def checkRequirements(fileName: String): Unit = {
    val metadata = Inputs.metadata(fileName)
    require(metadata.getOrElse("edges.canonical-orientation", "false").toBoolean,
      "Edges must be in canonical orientation")
    require(metadata.getOrElse("graph.remapped", "false").toBoolean,
      "Graph must be remapped with contiguous IDs")
  }

  def fromFile(fileName: String, layers: Int, interLayerWeight: Float): LayeredGraphGenerator = {
    new LayeredGraphGenerator(fileName, layers, interLayerWeight)
  }

}