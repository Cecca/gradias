package it.unipd.dei.gradias

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.weighted.{Bucket, BucketInfo}
import org.apache.spark.{Accumulator, SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Random

object AdjacencyGraph {

  def fromEdges[V:ClassTag](graph: RDD[(EdgeId, Distance)], initVertex: () => V)
  : AdjacencyGraph[V] = {
    val partitioner = new HashPartitioner(graph.sparkContext.defaultParallelism)
    val adjacencies = graph.map{ case ((u,v), w) =>
      (u, (v, w))
    }.groupByKey().mapValues { it =>
      val adj = it.toSeq
      new Adjacency(adj.map(_._1).toArray, adj.map(_._2).toArray)
    }.partitionBy(partitioner)
    val nodes = adjacencies.keys.map(v => (v, initVertex())).partitionBy(partitioner)
    new AdjacencyGraph(nodes, adjacencies)
  }


}

class AdjacencyGraph[V](
                        val nodes: RDD[(NodeId, V)],
                        val adjacencies: RDD[(NodeId, Adjacency)])
                      (implicit vt: ClassTag[V]) extends Graph[V] {

  override def sparkContext: SparkContext = nodes.sparkContext

  override def neighbourhoods(): RDD[(NodeId, Array[NodeId])] = adjacencies.mapValues(_.neighbours)

  override def persist(storageLevel: StorageLevel): AdjacencyGraph[V] =
    new AdjacencyGraph(nodes.persist(storageLevel), adjacencies.persist(storageLevel))

  override def unpersistEdges(blocking: Boolean = false): AdjacencyGraph[V] =
    new AdjacencyGraph(nodes = this.nodes, adjacencies = adjacencies.unpersist(blocking))

  override def unpersistNodes(blocking: Boolean = false): AdjacencyGraph[V] =
    new AdjacencyGraph(nodes = nodes.unpersist(blocking), adjacencies = this.adjacencies)

  override def isCheckpointed: Boolean = nodes.isCheckpointed

  override def checkpoint(): Unit = {
    algoLogger.info(yellow("__Checkpoint__"))
    nodes.checkpoint()
    adjacencies.checkpoint()
  }

  override def setName(name: String): AdjacencyGraph[V] = {
    nodes.setName(s"$name (nodes ${nodes.id}})")
    adjacencies.setName(s"$name (adjs ${adjacencies.id}})")
    this
  }

  override def mapNodes[V2: ClassTag](func: (NodeId, V) => V2): AdjacencyGraph[V2] = {
    val newNodes: RDD[(NodeId, V2)] = nodes.mapPartitions({ values =>
      values.map{case (id, v) => (id, func(id, v))}
    }, preservesPartitioning = true)
    require(newNodes.partitioner.isDefined,
      "The partitioner should be always defined")
    new AdjacencyGraph[V2](newNodes, this.adjacencies)
  }

  override def countMatching(pred: (NodeId, V) => Boolean): Long =
    nodes.filter(pred.tupled).count()

  override lazy val numNodes: Long = nodes.count()

  override lazy val numEdges: Long = adjacencies.map(_._2.size).reduce(_ + _) / 2

  override def genMessages[M: ClassTag](
                                         shouldSend: (NodeId, V) => Boolean,
                                         shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                         makeMessage: (NodeId, V, NodeId, Distance) => M,
                                         mergeMessages: (M, M) => M,
                                         messagesCounter: Accumulator[Long])
  : RDD[(NodeId, M)] = {
    val sendingNodes = nodes.filter(shouldSend.tupled)
    require(sendingNodes.partitioner.isDefined && sendingNodes.partitioner == adjacencies.partitioner,
      s"nodes: ${sendingNodes.partitioner}, edges ${adjacencies.partitioner}")

    sendingNodes.join(adjacencies).mapPartitions { it =>
      val msgMap = new Int2ObjectOpenHashMap[M]()
      it.foreach { case (id, (vertex, adj)) =>
        var i = 0
        while (i < adj.size) {
          val dst = adj.neighbours(i)
          val w = adj.weights(i)
          if(shouldTransmit(id, dst, w)) {
            messagesCounter += 1
            if(msgMap.containsKey(dst)) {
              msgMap.put(dst,
                mergeMessages(msgMap.get(dst), makeMessage(id, vertex, dst, w)))
            } else {
              msgMap.put(dst, makeMessage(id, vertex, dst, w))
            }
          }
          i += 1
        }
      }
      val keyIterator = msgMap.keySet().iterator()
      new Iterator[(NodeId, M)] {
        override def hasNext: Boolean = keyIterator.hasNext

        override def next(): (NodeId, M) = {
          val k = keyIterator.nextInt()
          (k, msgMap.get(k))
        }
      }
    }
      .combineByKey(v => v, mergeMessages, mergeMessages,
        partitioner = nodes.partitioner.get, mapSideCombine = false)
  }

  override def joinMessages[V2: ClassTag, M: ClassTag](messages: RDD[(NodeId, M)])
                                             (joinFunc: ((V, Option[M])) => V2)
  : AdjacencyGraph[V2] = {
    require(nodes.partitioner == messages.partitioner,
      "For efficiency, nodes and messages should be partitioned in the same way." +
        s"nodes: ${nodes.partitioner}, messages ${messages.partitioner}")
    val newNodes = nodes.leftOuterJoin(messages).mapValues(joinFunc)
    new AdjacencyGraph(newNodes, this.adjacencies)
  }

  override def exchangeMessages[V2: ClassTag, M: ClassTag](
                                                            shouldSend: (NodeId, V) => Boolean,
                                                            shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                                            makeMessage: (NodeId, V, NodeId, Distance) => M,
                                                            mergeMessages: (M, M) => M,
                                                            joinFunc: ((V, Option[M])) => V2,
                                                            messagesCounter: Accumulator[Long])
  : AdjacencyGraph[V2] = {
    val messages = genMessages(shouldSend, shouldTransmit, makeMessage, mergeMessages, messagesCounter)
    joinMessages(messages)(joinFunc)
  }


  override def toString: String =
    s"AdjacencyGraph(${nodes.name}, ${adjacencies.name}})"

}

class Adjacency(val neighbours: Array[NodeId], val weights: Array[Distance]) {
  def size = neighbours.length
}
