package it.unipd.dei.gradias

import it.unipd.dei.gradias.weighted.{Bucket, BucketInfo}
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

object Graph {

  def fromEdges[V: ClassTag](
                              graph: RDD[(EdgeId, Distance)],
                              initVertex: () => V,
                              implementation: String): Graph[V] = implementation match {
    case "edges" => EdgeGraph.fromEdges(graph, initVertex)
    case "adjacency" => AdjacencyGraph.fromEdges(graph, initVertex)
    case "blocks" => BlockGraph.fromEdges(graph, initVertex)
    case _ => throw new IllegalArgumentException(s"Unsupported implementation $implementation")
  }

  private var checkpointCnt = 1

}

trait Graph[V] extends Serializable {

  def sparkContext: SparkContext

  def nodes: RDD[(NodeId, V)]

  def neighbourhoods(): RDD[(NodeId, Array[NodeId])]

  def persist(storageLevel: StorageLevel): Graph[V]

  def unpersistEdges(blocking: Boolean = false): Graph[V]

  def unpersistNodes(blocking: Boolean = false): Graph[V]

  def checkpointProbability(prob: Double = 1.0): Unit = {
    if (Random.nextDouble() <= prob) {
      checkpoint()
    }
  }

  def checkpointEvery(interval: Int): Unit = {
    if(Graph.checkpointCnt % interval == 0) {
      checkpoint()
    }
    Graph.checkpointCnt += 1
  }

  def checkpoint(): Unit

  def isCheckpointed: Boolean

  def setName(name: String): Graph[V]

  def mapNodes[V2: ClassTag](func: (NodeId, V) => V2): Graph[V2]

  def numNodes: Long

  def numEdges: Long

  def genMessages[M: ClassTag](
                                shouldSend: (NodeId, V) => Boolean,
                                shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                makeMessage: (NodeId, V, NodeId, Distance) => M,
                                mergeMessages: (M, M) => M,
                                messageCounter: Accumulator[Long]): RDD[(NodeId, M)]

  def joinMessages[V2: ClassTag, M: ClassTag](messages: RDD[(NodeId, M)])
                                             (joinFunc: ((V, Option[M])) => V2): Graph[V2]

  def exchangeMessages[V2: ClassTag, M: ClassTag](
                                                   shouldSend: (NodeId, V) => Boolean,
                                                   shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                                   makeMessage: (NodeId, V, NodeId, Distance) => M,
                                                   mergeMessages: (M, M) => M,
                                                   joinFunc: ((V, Option[M])) => V2,
                                                   messageCounter: Accumulator[Long]): Graph[V2]

  def countMatching(pred: (NodeId, V) => Boolean): Long

}

object GraphImplicits {

  implicit def graphToCenterCounter[V <: CenterInfo](graph: Graph[V]): CenterCounter[V] =
    new CenterCounter[V](graph)

  implicit def graphToUpdatedCounter[V <: UpdatableVertex](graph: Graph[V]): UpdatedCounter[V] =
    new UpdatedCounter[V](graph)

  implicit def graphToBucketUpdatedCounter[V <: UpdatableVertex with BucketInfo](graph: Graph[V])
  : BucketUpdatedCounter[V] =
    new BucketUpdatedCounter[V](graph)

  implicit def graphToBucketCounter[V <: BucketInfo](graph: Graph[V])
  : BucketCounter[V] =
    new BucketCounter[V](graph)

  implicit def graphToQuotientCounter[V <: QuotientInfo](graph: Graph[V]): QuotientCounter[V] =
    new QuotientCounter[V](graph)

  implicit def graphToCoveringCounter[V <: CoveringInfo](graph: Graph[V]): CoveringCounter[V] =
    new CoveringCounter[V](graph)

  class CenterCounter[V <: CenterInfo](graph: Graph[V]) {

    def countCenters(): Long = graph.countMatching((id, v) => v.isCenter)

  }

  class UpdatedCounter[V <: UpdatableVertex](graph: Graph[V]) {

    def countUpdated(): Long = graph.countMatching((id, v) => v.updated)

  }

  class BucketCounter[V <: BucketInfo](graph: Graph[V]) {

    def countInBucket(bucket: Bucket): Long = graph.countMatching((id, v) => bucket.contains(v))

  }

  class BucketUpdatedCounter[V <: UpdatableVertex with BucketInfo](graph: Graph[V]) {

    def countBucketUpdated(bucket: Bucket): Long =
      graph.countMatching((id, v) => v.updated && bucket.contains(v))

  }

  class QuotientCounter[V <: QuotientInfo](graph: Graph[V]) {

    def quotientSize(): Long = graph.countMatching((id, v) => v.isQuotient)

  }

  class CoveringCounter[V <: CoveringInfo](graph: Graph[V]) {

    def countUncovered(): Long = graph.countMatching((id, v) => v.isUncovered)

  }

}
