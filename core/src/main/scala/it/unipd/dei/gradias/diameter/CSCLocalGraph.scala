package it.unipd.dei.gradias.diameter

import java.nio.{ByteBuffer, IntBuffer}

import it.unimi.dsi.logging.ProgressLogger
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.{NodeId, Distance}
import it.unipd.dei.gradias.io.{EdgeKey, EdgeLongWeight, Inputs, Transformers}
import it.unipd.dei.gradias.util.RunningStats
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, NullWritable}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.spark.graphx.VertexId

import scala.compat.Platform

/**
 * Local sparse graph representation using Compressed Sparse Column matrix
 */
class CSCLocalGraph private (
                              val n: Int,
                              val nonZero: Int,
                              val columnPointers: Array[Int],
                              val rowIndices: Array[Int],
                              val weights: Array[Distance]) {

  def inspect(): Unit = {
    var j = 0
    while(j < n) {
      val pStart = columnPointers(j)
      val pEnd = columnPointers(j+1)
      var i = pStart
      while(i < pEnd) {
        println(s"(${rowIndices(i)}, $j) : ${weights(i)}")
        i += 1
      }
      j += 1
    }
  }

  lazy val stats = {
    val weightStats = new RunningStats()
    val degreeStats = new RunningStats()

    var j = 0
    while(j < n) {
      val pStart = columnPointers(j)
      val pEnd = columnPointers(j+1)
      var i = pStart
      while (i < pEnd) {
        if (rowIndices(i) < j) {
          val w = weights(i).toDouble
          weightStats.push(w)
        }
        degreeStats.push(pEnd - pStart)
        i += 1
      }
      j += 1
    }

    Map(
      "nodes" -> n,
      "edges" -> weightStats.count,
      "degree-mean" -> degreeStats.mean,
      "degree-variance" -> degreeStats.variance,
      "degree-stddev" -> degreeStats.stddev,
      "degree-max" -> degreeStats.max,
      "degree-min" -> degreeStats.min,
      "weight-mean" -> weightStats.mean,
      "weight-variance" -> weightStats.variance,
      "weight-stddev" -> weightStats.stddev,
      "weight-max" -> weightStats.max,
      "weight-min" -> weightStats.min
    )
  }


}

object CSCLocalGraph {

  def apply(n: Int, nonZero: Int): CSCLocalGraph =
    new CSCLocalGraph(
      n,
      nonZero,
      Array.ofDim(n+1),
      Array.ofDim(nonZero),
      Array.ofDim(nonZero))

  def apply(edges: Iterator[(VertexId, VertexId, Distance)]): CSCLocalGraph = {
    val builder = new CSCLocalGraphBuilder()
    for((u,v,w) <- edges) {
      builder.add(u.toInt, v.toInt, w)
    }
    builder.freeze()
  }

  def fromSmallIdEdges(edges: Iterator[(NodeId, NodeId, Distance)]): CSCLocalGraph = {
    val builder = new CSCLocalGraphBuilder()
    for((u,v,w) <- edges) {
      builder.add(u, v, w)
    }
    builder.freeze()
  }

  def fromSequenceFile(path: String): CSCLocalGraph = {
    algoLogger.info(s"Loading graph from $path")

    val builder = new CSCLocalGraphBuilder()

    val canonicalOrientation = Inputs.metadata(path).getOrElse("edges.canonical-orientation", "false").toBoolean

    val start = System.currentTimeMillis()

    var cnt = 0
    val pl = new ProgressLogger(algoLogger, "edges")
    pl.displayFreeMemory = true
    if(canonicalOrientation){
      algoLogger.info("Loading graph with canonical edge orientation")
    }

    val inputGraph = Inputs.sequenceFile(path)

    pl.start()
    for((eKey, eWeight) <- inputGraph) {
      pl.lightUpdate()

      val iSrc = eKey.src.toInt
      val iDst = eKey.dst.toInt
      val weight = eWeight

      builder.add(iSrc, iDst, weight)
      if(canonicalOrientation) {
        builder.add(iDst, iSrc, weight)
      }

      cnt += 1
    }
    pl.stop()

    algoLogger.info(s"Read $cnt lines")
    algoLogger.info("Freezing graph")
    val graph = builder.freeze()
    val end = System.currentTimeMillis()
    algoLogger.info(s"Loaded graph in {} seconds", (end-start) / 1000.0)
    graph
  }

}

class CSCLocalGraphBuilder {
  import CSCLocalGraphBuilder.cumsum

  var n = 0

  var nonZeroMax = 8192
  var nonZero = 0

  var columns: Array[Int] = Array.ofDim(nonZeroMax)
  var rows: Array[Int] = Array.ofDim(nonZeroMax)
  var values: Array[Distance] = Array.ofDim(nonZeroMax)

  def add(i: Int, j: Int, value: Distance) = {
    ensureCapacity(nonZero)
    values(nonZero) = value
    rows(nonZero) = i
    columns(nonZero) = j
    nonZero += 1

    n = math.max(i+1, n)
    n = math.max(j+1, n)
  }

  def ensureCapacity(size: Int) = {
    if(nonZero >= nonZeroMax) {
      if (nonZeroMax * 4 < nonZeroMax) {
        algoLogger.info("Quadrupling the array size would make it overflow. Using Int.MaxValue instead")
        nonZeroMax = Int.MaxValue - 2 // Otherwise we have a OutOfMemoryError
      } else {
        nonZeroMax = nonZeroMax * 4
      }
      Platform.collectGarbage()

      algoLogger.info(s"Increasing the size of the columns array: $nonZeroMax")
      val newColumns = Array.ofDim[Int](nonZeroMax)
      Platform.arraycopy(columns, 0, newColumns, 0, nonZero)
      columns = newColumns

      algoLogger.info(s"Increasing the size of the rows array: $nonZeroMax")
      val newRows = Array.ofDim[Int](nonZeroMax)
      Platform.arraycopy(rows, 0, newRows, 0, nonZero)
      rows = newRows

      algoLogger.info(s"Increasing the size of the values array: $nonZeroMax")
      val newValues = Array.ofDim[Distance](nonZeroMax)
      Platform.arraycopy(values, 0, newValues, 0, nonZero)
      values = newValues
      Platform.collectGarbage()
    }
  }

  def trim(size: Int): Unit = {
    if (size < nonZeroMax) {
      Platform.collectGarbage()

      algoLogger.info(s"Trimming the size of the columns array: $size")
      val newColumns = Array.ofDim[Int](size)
      Platform.arraycopy(columns, 0, newColumns, 0, size)
      columns = newColumns

      algoLogger.info(s"Trimming the size of the rows array: $size")
      val newRows = Array.ofDim[Int](size)
      Platform.arraycopy(rows, 0, newRows, 0, size)
      rows = newRows

      algoLogger.info(s"Trimming the size of the values array: $size")
      val newValues = Array.ofDim[Distance](size)
      Platform.arraycopy(values, 0, newValues, 0, size)
      values = newValues

      Platform.collectGarbage()
    }
  }

  def freeze(): CSCLocalGraph = {
    trim(nonZero)
    algoLogger.info("Allocating memory for CSCLocalGraph")
    val res = CSCLocalGraph(n, nonZero)
    algoLogger.info("Allocating memory for the workspace")
    val workspace = Array.ofDim[Int](n)

    algoLogger.info("count non-zero elements per column")
    var k = 0
    while(k < nonZero) {
      workspace(columns(k)) += 1
      k += 1
    }
    algoLogger.info("set column pointers")
    cumsum(res.columnPointers, workspace, n)

    algoLogger.info("copy elements")
    k = 0
    while (k < nonZero) {
      val p = workspace(columns(k))
      workspace(columns(k)) += 1
      res.rowIndices(p) = rows(k)
      require(values(k) > 0.0)
      res.weights(p) = values(k)
      k += 1
    }
    algoLogger.info("Complete freezing")
    res
  }

  def inspect(): Unit = {
    var i = 0
    while(i<nonZero) {
      println(s"(${rows(i)}, ${columns(i)}) : ${values(i)}")
      i += 1
    }
  }

}

object CSCLocalGraphBuilder {

  def cumsum(result: Array[Int], addends: Array[Int], len: Int) = {
    var i = 0
    var cnt = 0
    while(i < len) {
      result(i) += cnt
      cnt += addends(i)
      addends(i) = result(i)
      i += 1
    }
    result(len) = cnt
  }

}

