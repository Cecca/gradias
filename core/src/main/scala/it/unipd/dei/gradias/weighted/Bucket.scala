package it.unipd.dei.gradias.weighted

import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.Logger._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

case class Bucket(delta: Distance, index: Int) {

  val minDistance: Distance = index * delta

  val maxDistance: Distance = (index + 1) * delta

  def isLight(edgeWeight: Distance): Boolean = edgeWeight <= delta

  def contains(vertexInfo: BucketInfo): Boolean =
    minDistance <= vertexInfo.bucketDistance() && vertexInfo.bucketDistance() < maxDistance

  override def toString: String =
    s"Bucket(index = $index, delta = $delta, from=$minDistance, to = $maxDistance)"

  def bucketString[V <: BucketInfo](graph: RDD[(NodeId, V)], compact: Boolean = false)
                                   (implicit tag: ClassTag[V]): String = {
    val collectedBuckets = graph.values.map { vertex =>
      if (vertex.bucketDistance() == Infinity)
        (Infinity, 1)
      else
        (vertex.bucketDistance() / delta, 1) // the bucket ID
    }.reduceByKey(_ + _).collect().sortBy(_._1)

    val buckets =
      if(compact && collectedBuckets.length > 3) {
        val reduced = collectedBuckets.takeWhile(_._1 < this.index)
          .fold((0f,0)) { (a, b) => (math.max(a._1, b._1), a._2 + b._2)}
        val head = cyan(s"<=${reduced._1}") + ":" + green(reduced._2)
        val tail = collectedBuckets.dropWhile(_._1 < this.index).map{ case (bucketIdx, cnt) =>
          s"${cyan(Distance.toString(bucketIdx))}:${green(cnt)}"
        }
        head +: tail
      } else {
        collectedBuckets.map{ case (bucketIdx, cnt) =>
          s"${cyan(Distance.toString(bucketIdx))}:${green(cnt)}"
        }
      }

    val bString = buckets.mkString("| ", " | ", " |")

    bString
  }

  def logBucketString[V <: BucketInfo](graph: RDD[(NodeId, V)], compact: Boolean = false)
                                      (implicit tag: ClassTag[V]): Unit = {
    if(System.getProperty("gradias.log.buckets", "false").toBoolean) {
      algoLogger.info(s"buckets ${this.bucketString(graph, compact = true)}")
    }
  }

}

/**
 * Trait to be applied to a vertex, to provide information about
 * the bucket (potentially) containing it.
 */
trait BucketInfo {

  /**
   * A vertex may have more than one associated distance measure.
   * This method returns the one relevant to bucket computations.
   *
   * @return the distance relevant to bucket computations.
   */
  def bucketDistance(): Distance

}
