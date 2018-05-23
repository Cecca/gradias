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

object EdgeGraph {

  def fromEdges[V:ClassTag](graph: RDD[(EdgeId, Distance)], initVertex: () => V)
  : EdgeGraph[V] = {
    val partitioner = new HashPartitioner(graph.sparkContext.defaultParallelism)
    val edges = graph.map{case ((u,v), w) =>
      (u, (v, w))
    }.partitionBy(partitioner)
    val nodes = edges.keys.distinct().map(v => (v, initVertex())).partitionBy(partitioner)
    new EdgeGraph(nodes, edges)
  }

}

class EdgeGraph[V](
                    val nodes: RDD[(NodeId, V)],
                    val edges: RDD[(NodeId, (NodeId, Distance))])
                  (implicit vt: ClassTag[V]) extends Graph[V] {

  override def sparkContext: SparkContext = nodes.sparkContext

  override def neighbourhoods(): RDD[(NodeId, Array[NodeId])] =
    edges.mapValues(_._1).groupByKey().mapValues(_.toArray)

  override def persist(storageLevel: StorageLevel): EdgeGraph[V] =
    new EdgeGraph(nodes.persist(storageLevel), edges.persist(storageLevel))

  override def unpersistEdges(blocking: Boolean = false): EdgeGraph[V] =
    new EdgeGraph(nodes = this.nodes, edges = edges.unpersist(blocking))

  override def unpersistNodes(blocking: Boolean = false): EdgeGraph[V] =
    new EdgeGraph(nodes = nodes.unpersist(blocking), edges = this.edges)

  override def isCheckpointed: Boolean = nodes.isCheckpointed

  override def checkpoint(): Unit = {
    algoLogger.info(yellow("__Checkpoint__"))
    nodes.checkpoint()
    edges.checkpoint()
  }

  override def setName(name: String): EdgeGraph[V] = {
    nodes.setName(s"$name (nodes ${nodes.id}})")
    edges.setName(s"$name (edges ${edges.id}})")
    this
  }

  override def mapNodes[V2: ClassTag](func: (NodeId, V) => V2): EdgeGraph[V2] = {
    val newNodes: RDD[(NodeId, V2)] = nodes.mapPartitions({ values =>
      values.map{case (id, v) => (id, func(id, v))}
    }, preservesPartitioning = true)
    require(newNodes.partitioner.isDefined,
      "The partitioner should be always defined")
    new EdgeGraph[V2](newNodes, this.edges)
  }

  override lazy val numNodes: Long = nodes.count()

  override lazy val numEdges: Long = edges.count()

  override def countMatching(pred: (NodeId, V) => Boolean): Long =
    nodes.filter(pred.tupled).count()

  override def genMessages[M: ClassTag](
                                         shouldSend: (NodeId, V) => Boolean,
                                         shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                         makeMessage: (NodeId, V, NodeId, Distance) => M,
                                         mergeMessages: (M, M) => M,
                                         messageCounter: Accumulator[Long])
  : RDD[(NodeId, M)] = {
    val sendingNodes = nodes.filter(shouldSend.tupled)
    val transmittingEdges = edges.filter(e => shouldTransmit(e._1, e._2._1, e._2._2))
    require(sendingNodes.partitioner.isDefined && sendingNodes.partitioner == transmittingEdges.partitioner,
      s"nodes: ${sendingNodes.partitioner}, edges ${transmittingEdges.partitioner}")

    sendingNodes.join(transmittingEdges).mapPartitions{ it =>
      val msgMap = new Int2ObjectOpenHashMap[M]()
      it.foreach { case (id, (vertex, (destination, weight))) =>
      messageCounter += 1
        if(msgMap.containsKey(destination)) {
          msgMap.put(destination,
            mergeMessages(msgMap.get(destination), makeMessage(id, vertex, destination, weight)))
        } else {
          msgMap.put(destination, makeMessage(id, vertex, destination, weight))
        }
      }
//      it.map { case (id, (vertex, (destination, weight))) =>
//        (destination, makeMessage(id, vertex, destination, weight))
//      }
      val keyIterator = msgMap.keySet().iterator()
      new Iterator[(NodeId, M)] {
        override def hasNext: Boolean = keyIterator.hasNext

        override def next(): (NodeId, M) = {
          val k = keyIterator.nextInt()
          (k, msgMap.get(k))
        }
      }
    }
//      .reduceByKey(mergeMessages)
      .combineByKey(v => v, mergeMessages, mergeMessages,
        partitioner = nodes.partitioner.get, mapSideCombine = false)
  }

  override def joinMessages[V2: ClassTag, M: ClassTag](messages: RDD[(NodeId, M)])
                                                      (joinFunc: ((V, Option[M])) => V2)
  : EdgeGraph[V2] = {
    require(nodes.partitioner == messages.partitioner,
      "For efficiency, nodes and messages should be partitioned in the same way." +
        s"nodes: ${nodes.partitioner}, messages ${messages.partitioner}")
    val newNodes = nodes.leftOuterJoin(messages).mapValues(joinFunc)
    new EdgeGraph(newNodes, this.edges)
  }

  override def exchangeMessages[V2: ClassTag, M: ClassTag](
                                                            shouldSend: (NodeId, V) => Boolean,
                                                            shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                                            makeMessage: (NodeId, V, NodeId, Distance) => M,
                                                            mergeMessages: (M, M) => M,
                                                            joinFunc: ((V, Option[M])) => V2,
                                                            messageCounter: Accumulator[Long]): EdgeGraph[V2] = {
    val messages = genMessages(shouldSend, shouldTransmit, makeMessage, mergeMessages, messageCounter)
    joinMessages(messages)(joinFunc)
  }

  override def toString: String =
    s"EdgeGraph(${nodes.name}, ${edges.name}})"

}
