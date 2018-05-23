package it.unipd.dei.gradias.weighted

import it.unipd.dei.gradias._
import it.unipd.dei.gradias.io.{EdgeKey, EdgeLongWeight}
import it.unipd.dei.gradias.util.Logger.algoLogger
import org.apache.hadoop.io.{FloatWritable, NullWritable}
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object WeightedGraphLoader {

  def load(sc: SparkContext, path: String, edgeCounter: Accumulator[Long], persistLevel: StorageLevel)
  : RDD[(EdgeId, Distance)] = {
    load(sc, path, edgeCounter).persist(persistLevel)
  }

  def load(sc: SparkContext, path: String, edgeCounter: Accumulator[Long])
  : RDD[(EdgeId, Distance)] = {
    if (path.endsWith(".graph")) {
      algoLogger.info(s"Loading graph from sequence file $path")
      sc.sequenceFile(path, classOf[EdgeKey], classOf[FloatWritable], minPartitions = sc.defaultParallelism)
        .map{ case (eKey, eWeight) =>
          edgeCounter += 1
          ((eKey.src.toInt, eKey.dst.toInt), eWeight.get()): (EdgeId, Distance)
        }
        .setName("Input")
    } else {
      algoLogger.info(s"Loading graph from text file $path")
      sc.textFile(path).map { line =>
        val comps = line.split("\\s+")
        if (comps.length != 3) {
          throw new IllegalArgumentException("Each line should contain the endpoints and the weight")
        }
        edgeCounter += 1
        ((comps(0).toInt, comps(1).toInt), comps(2).toFloat): (EdgeId, Distance)
      }.setName("Input")
    }

  }

}
