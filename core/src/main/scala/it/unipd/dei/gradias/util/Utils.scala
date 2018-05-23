package it.unipd.dei.gradias.util

import it.unipd.dei.gradias.io.StatsCollector
import it.unipd.dei.gradias.{EdgeId, Distance}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Generic utility functions
 */
object Utils {

  /**
   * Given a number of initial nodes and a target size, returns a
   * sequence of numbers representing the centers to activate at
   * each iteration.
   *
   * @param numNodes the initial number of nodes
   * @param targetSize the desired final size
   * @return a sequence of number of centers to activate
   */
  def centersSeq(numNodes: Long, targetSize: Long): Seq[Double] = {
    def numIt(n: Long): Int = {
      if(n <= targetSize)
        return 0
      else
        return 1 + numIt(n / 2)
    }
    val numIterations = numIt(numNodes)
    Logger.algoLogger.info("Number of iterations is " + numIterations)
    Array.fill(numIterations)(targetSize / numIterations.toDouble)
  }

  lazy val checkpointProbability: Double ={
    System.getProperty("gradias.checkpoint.probability", "0.02").toDouble
  }

  lazy val checkpointInterval: Int = {
    System.getProperty("gradias.checkpoint.interval", "100").toInt
  }

  val storageLevel: StorageLevel =
    System.getProperty("gradias.storageLevel", "MEMORY_ONLY") match {
      case "NONE" => StorageLevel.NONE
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case err => throw new IllegalArgumentException("Unsupported storage level " + err)
    }

  def getDelta(proposedDelta: String, graphFile: String, sc: SparkContext)
  : Distance = proposedDelta match {
    case "min" =>
      Logger.algoLogger.info("Computing minimum edge weight")
      val stats = StatsCollector.collect(graphFile, Some(sc))
      val m = stats("weightMin")
      Logger.algoLogger.info("Minimum edge weight is {}", m)
      m.toString.toFloat
    case maybeFloat => maybeFloat.toFloat
  }

}
