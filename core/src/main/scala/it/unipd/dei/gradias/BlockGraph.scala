package it.unipd.dei.gradias

import it.unimi.dsi.fastutil.ints._
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.{FloatArrayBuffer, IntArrayBuffer, Logger, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, HashPartitioner, Partitioner, SparkContext}

import scala.reflect.ClassTag

object BlockGraph {

  class SourceVertexPartitioner(override val numPartitions: Int) extends Partitioner {
    val hPart = new HashPartitioner(numPartitions)

    override def getPartition(key: Any): Int = key match {
      case (src, _) => hPart.getPartition(src)
      case _ => throw new RuntimeException("Only Tuple2 keys are supported")
    }
  }

  def fromEdges[V:ClassTag](
                             graph: RDD[(EdgeId, Distance)],
                             initVertex: () => V) = {
    val partitioner = new SourceVertexPartitioner(graph.sparkContext.defaultParallelism)
    val blocks = graph
      .partitionBy(partitioner)
      .mapPartitionsWithIndex ({ case (partId, edges) =>
      val idMap = new Int2IntOpenHashMap()
      val adjsBuffers = new Int2ObjectOpenHashMap[IntArrayBuffer]()
      val weightsBuffers = new Int2ObjectOpenHashMap[FloatArrayBuffer]()

      var cnt = 0
      for (((src, dst), w) <- edges) {
        if(!idMap.containsKey(src)) {
          idMap.put(src, cnt)
          cnt += 1
        }
        if (!adjsBuffers.containsKey(src)) {
          adjsBuffers.put(src, IntArrayBuffer(128))
          weightsBuffers.put(src, FloatArrayBuffer(128))
        }
        adjsBuffers.get(src).append(dst)
        weightsBuffers.get(src).append(w)
      }

      val attributes = Array.fill[V](idMap.size)(initVertex())
      val neighbours = Array.ofDim[Array[NodeId]](idMap.size)
      val weights = Array.ofDim[Array[Distance]](idMap.size)

      val adjIterator = adjsBuffers.keySet().iterator()
      while(adjIterator.hasNext) {
        val src = adjIterator.nextInt()
        val adj = adjsBuffers.get(src)
        val ws = weightsBuffers.get(src)
        neighbours(idMap.get(src)) = adj.toArray
        weights(idMap.get(src)) = ws.toArray
      }

      val identifiers = Array.ofDim[NodeId](idMap.size)
      val idIterator = idMap.keySet().iterator()
      while(idIterator.hasNext) {
        val node = idIterator.nextInt()
        val idx = idMap.get(node)
        identifiers(idx) = node
      }

      val attrBlock = new NodeAttributesBlock[V](identifiers, attributes)
      val adjBlock = new AdjacencyBlock(neighbours, weights)

      Iterator((partId, (attrBlock, adjBlock)))
    }, preservesPartitioning = true).persist(Utils.storageLevel)

    val attributes = blocks.mapValues(_._1).partitionBy(partitioner.hPart)
    val adjacencies = blocks.mapValues(_._2).partitionBy(partitioner.hPart)
    require(attributes.partitioner.get == adjacencies.partitioner.get,
      "Attributes and adjacencies do not have the same partitioning after construction")
    val g = new BlockGraph[V](attributes, adjacencies).setName("Graph").persist(Utils.storageLevel)
    g.numEdges
    g.numNodes
    blocks.unpersist()
    g
  }

}

/**
 * Represents a graph as a collection of blocks.
 */
class BlockGraph[V](
                     val attributes: RDD[(Int, NodeAttributesBlock[V])],
                     val adjacencies: RDD[(Int, AdjacencyBlock)])
extends Graph[V] {

  override def sparkContext: SparkContext = attributes.sparkContext

  override def numNodes: Long = attributes.map(_._2.numNodes).reduce(_ + _)

  override def numEdges: Long = adjacencies.map(_._2.numEdges).reduce(_ + _) / 2

  override def neighbourhoods(): RDD[(NodeId, Array[NodeId])] =
    throw new UnsupportedOperationException()

  override def nodes: RDD[(NodeId, V)] = attributes.flatMap {
    case (_, block) => block.nodePairs
  }

  override def persist(storageLevel: StorageLevel): Graph[V] =
    new BlockGraph[V](this.attributes.persist(storageLevel), this.adjacencies.persist(storageLevel))

  override def unpersistNodes(blocking: Boolean): Graph[V] =
    new BlockGraph[V](this.attributes.unpersist(blocking), adjacencies)

  override def unpersistEdges(blocking: Boolean): Graph[V] = {
    Logger.algoLogger.warn("Unpersisting edges")
    new BlockGraph[V](attributes, adjacencies.unpersist(blocking))
  }

  override def setName(name: String): Graph[V] =
    new BlockGraph[V](
      this.attributes.setName(name + " (attributes)"),
      this.adjacencies.setName(name + " (adjacencies)"))


  override def isCheckpointed: Boolean = attributes.isCheckpointed

  override def checkpoint(): Unit = {
    algoLogger.info(yellow("__Checkpoint__"))
    attributes.checkpoint()
  }

  override def countMatching(pred: (NodeId, V) => Boolean): Long =
    attributes.map(_._2.countMatching(pred)).reduce(_ + _)

  override def mapNodes[V2: ClassTag](func: (NodeId, V) => V2): Graph[V2] =
    new BlockGraph[V2](attributes.mapValues(_.mapNodes(func)), adjacencies)

  override def genMessages[M: ClassTag](
                                         shouldSend: (NodeId, V) => Boolean,
                                         shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                         makeMessage: (NodeId, V, NodeId, Distance) => M,
                                         mergeMessages: (M, M) => M,
                                         messageCounter: Accumulator[Long]): RDD[(NodeId, M)] =
    throw new UnsupportedOperationException("Use exchangeMessages instead.")

  override def joinMessages[V2: ClassTag, M: ClassTag](messages: RDD[(NodeId, M)])
                                                      (joinFunc: ((V, Option[M])) => V2): Graph[V2] =
    throw new UnsupportedOperationException("Use exchangeMessages instead.")

  override def exchangeMessages[V2: ClassTag, M: ClassTag](
                                                            shouldSend: (NodeId, V) => Boolean,
                                                            shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                                                            makeMessage: (NodeId, V, NodeId, Distance) => M,
                                                            mergeMessages: (M, M) => M,
                                                            joinFunc: ((V, Option[M])) => V2,
                                                            messageCounter: Accumulator[Long])
  : Graph[V2] = {

    val partitioner = attributes.partitioner.get
    require(partitioner == adjacencies.partitioner.get,
      "For efficiency, attributes and adjacencies must be partitioned in the same way")

    val messages = attributes.join(adjacencies).flatMap { case (blockId, (attrBlock, adjBlock)) =>
      val msgs = attrBlock.genMessages(adjBlock)(partitioner, shouldSend, shouldTransmit, makeMessage, mergeMessages)
      for(i <- msgs.indices) yield {
        val ms = msgs(i)
        messageCounter += ms.size()
        (i, ms)
      }
    }.reduceByKey(NodeAttributesBlock.mergeMaps(mergeMessages))

    val updatedBlocks = attributes.leftOuterJoin(messages).mapValues {
      case (attrBlock, Some(msgMap)) => attrBlock.joinMessages(joinFunc, msgMap)
      case (attrBlock, None) => attrBlock.joinMessages(joinFunc, new Int2ObjectOpenHashMap[M]())
    }

    new BlockGraph[V2](updatedBlocks, adjacencies)
  }

}

object NodeAttributesBlock {
  
  def mergeMaps[M](mergeFunc: (M, M) => M)(m1: Int2ObjectMap[M], m2: Int2ObjectMap[M]): Int2ObjectMap[M] = {
    val nodeIt = m2.keySet().iterator()
    while(nodeIt.hasNext) {
      val id = nodeIt.nextInt()
      putOrMerge(m1, id, m2.get(id), mergeFunc)
    }
    m1
  }

  def putOrMerge[M](m: Int2ObjectMap[M], k: Int, elem: M, mergeFunc: (M, M) => M): Unit = {
    if(m.containsKey(k)) {
      m.put(k, mergeFunc(elem, m.get(k)))
    } else {
      m.put(k, elem)
    }
  }

}

class NodeAttributesBlock[V](
                              val identifiers: Array[NodeId],
                              val attributes: Array[V]) extends Serializable {

  def numNodes: Long = attributes.length

  def countMatching(pred: (NodeId, V) => Boolean): Long = {
    var cnt = 0L
    var idx = 0
    while (idx < identifiers.length) {
      if (pred(identifiers(idx), attributes(idx))) {
        cnt += 1L
      }
      idx += 1
    }
    cnt
  }

  def mapNodes[V2: ClassTag](func: (NodeId, V) => V2): NodeAttributesBlock[V2] = {
    val newAttributes = Array.ofDim[V2](identifiers.length)
    var idx = 0
    while (idx < identifiers.length) {
      val nodeId = identifiers(idx)
      newAttributes(idx) = func(nodeId, attributes(idx))
      idx += 1
    }

    val newBlock = new NodeAttributesBlock[V2](
      identifiers, newAttributes)

    newBlock
  }

  def nodePairs: Iterator[(NodeId, V)] = identifiers.zipWithIndex.iterator.map {
    case (nodeId, idx) => (nodeId, attributes(idx))
  }

  def genMessages[M:ClassTag](adjacencyBlock: AdjacencyBlock)
                             (
                               partitioner: Partitioner,
                               shouldSend: (NodeId, V) => Boolean,
                               shouldTransmit: (NodeId, NodeId, Distance) => Boolean,
                               makeMessage: (NodeId, V, NodeId, Distance) => M,
                               mergeMessages: (M, M) => M)
  : IndexedSeq[Int2ObjectMap[M]] = {
    val messages = Array.fill(partitioner.numPartitions)(new Int2ObjectOpenHashMap[M]())

    var idx = 0
    while (idx < identifiers.length) {
      val source = identifiers(idx)
      val nodeAttr = attributes(idx)
      if(shouldSend(source, nodeAttr)) {
        var i = 0
        val maxI = adjacencyBlock.neighbours(idx).length
        while (i < maxI) {
          val dest = adjacencyBlock.neighbours(idx)(i)
          val w = adjacencyBlock.weights(idx)(i)
          if(shouldTransmit(source, dest, w)) {
            val msg = makeMessage(source, nodeAttr, dest, w)
            val pid = partitioner.getPartition(dest)
            NodeAttributesBlock.putOrMerge(messages(pid), dest, msg, mergeMessages)
          }
          i += 1
        }
      }
      idx += 1
    }

    messages
  }

  def joinMessages[V2:ClassTag, M:ClassTag](
                                             joinFunc: ((V, Option[M])) => V2,
                                             messages: Int2ObjectMap[M])
  : NodeAttributesBlock[V2] = {
    val newAttributes = Array.ofDim[V2](attributes.length)
    var idx = 0
    while(idx < identifiers.length) {
      val nodeId = identifiers(idx)
      if(messages.containsKey(nodeId)) {
        newAttributes(idx) = joinFunc(attributes(idx), Some(messages.get(nodeId)))
      } else {
        newAttributes(idx) = joinFunc(attributes(idx), None)
      }
      idx += 1
    }
    new NodeAttributesBlock[V2](identifiers, newAttributes)
  }

//  def describe: String = {
//    val adjString = identifiers.zipWithIndex.sorted.map { case (node, idx) =>
//      s"$node -> ${deprNeighbours(idx).sorted.mkString("[", ", ", "]")}"
//    }.mkString("\n")
//    s"""
//        |nodes: ${identifiers.sorted}
//        |adjacencies:
//        |$adjString
//    """.
//      stripMargin
//  }

}

class AdjacencyBlock(
                      val neighbours: Array[Array[NodeId]],
                      val weights: Array[Array[Distance]]) extends Serializable {

  def numEdges: Long = neighbours.map(_.length).sum
}
