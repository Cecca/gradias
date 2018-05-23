/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.gradias

import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, Int2ReferenceArrayMap}
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet
import it.unipd.dei.gradias.Timer._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

import scala.reflect.ClassTag

class EdgeBuilder(val persistLevel: StorageLevel)
  extends Serializable{

  def buildEdges[V <: CenterInfo with DistanceInfo with NeighbourhoodInfo](graph: RDD[(NodeId, V)])
                                                                          (implicit tag: ClassTag[V])
  : RDD[(EdgeId, Distance)] = action("Edge construction") {
    val messages = graph.flatMap{ case (id, v) =>
      v.neighbourhood
        .filter(_ < id) // this way only one end of the edge is responsible for reweighting the edge
        .map((_, (v.center, v.distance)))
    }.setName("Edge messages").persist(persistLevel)

    val nodes = graph.mapValues(v => (v.center, v.distance))
      .setName("Distance info").persist(persistLevel)

    nodes.join(messages).flatMap {
      case ( _, ((v1, w1), (v2, w2)) ) =>
        if(v1 != v2)
          Seq(((v1, v2), w1 + w2 + 1))
        else
          Seq.empty
    }.reduceByKey(math.min)
      .setName("Edges").persist(persistLevel)
  }

  def buildEdges[V <: CenterInfo with DistanceInfo : ClassTag](graph: Graph[V])
  : RDD[(EdgeId, Distance)] = graph match {
    case g: AdjacencyGraph[V] => buildEdges(g)
    case g: BlockGraph[V] => buildEdges(g)
  }

  def buildEdges[V <: CenterInfo with DistanceInfo : ClassTag](graph: BlockGraph[V])
  : RDD[(EdgeId, Distance)] = {

    val partitioner = graph.attributes.partitioner.get

    val msgs = graph.attributes.join(graph.adjacencies).flatMap { case (blockId, (attrBlock, adjBlock)) =>
      val toSend = Array.fill(partitioner.numPartitions)(new Int2ObjectOpenHashMap[(NodeId, Distance)]())
      var idx = 0
      while (idx < attrBlock.identifiers.length) {
        val src = attrBlock.identifiers(idx)
        val adj = adjBlock.neighbours(idx)
        adj foreach { dest =>
          if (src < dest) {
            val partDest = partitioner.getPartition(dest)
            val attr = attrBlock.attributes(idx)
            toSend(partDest).put(src, (attr.center, attr.distance))
          }
        }
        idx += 1
      }
      toSend.indices.iterator.map { i =>
        (i, toSend(i))
      }
    }.reduceByKey { (a, b) =>
      a.putAll(b)
      a
    }

    graph.attributes.join(graph.adjacencies).join(msgs).flatMap { case (blockId, ((attrBlock, adjBlock), nodes)) =>
      val edges = mutable.Map[EdgeId, Distance]()
      var idx = 0
      while (idx < attrBlock.identifiers.length) {
        val srcId = attrBlock.identifiers(idx)
        var i = 0
        while(i < adjBlock.neighbours(idx).length) {
          val srcAttr = attrBlock.attributes(idx)
          val dstId = adjBlock.neighbours(idx)(i)
          if (srcId > dstId) {
            assert(nodes.containsKey(dstId), s"Missing key $dstId (source $srcId)")
            val (dstCenter, dstDistance) = nodes.get(dstId)
            if (srcAttr.center != dstCenter) {
              val edgeId =
                if (srcAttr.center < dstCenter) (srcAttr.center, dstCenter)
                else (dstCenter, srcAttr.center)
              val newWeight = srcAttr.distance + dstDistance + adjBlock.weights(idx)(i)
              edges.get(edgeId) match {
                case None => edges.put(edgeId, newWeight)
                case Some(oldWeight) =>
                  if (newWeight < oldWeight) {
                    edges.put(edgeId, newWeight)
                  }
              }
            }
          }
          i += 1
        }
        idx += 1
      }
      edges.iterator
    }.reduceByKey(math.min)
  }

  def buildEdges[V <: CenterInfo with DistanceInfo : ClassTag](graph: AdjacencyGraph[V])
  : RDD[(EdgeId, Distance)] = {
    val partitioner = graph.nodes.partitioner.get

    val attributes = graph.nodes.mapPartitionsWithIndex({ (idx, it) =>
      val attrs = new Int2ObjectOpenHashMap[V]()
      it.foreach { case (v, attr) => attrs.put(v, attr) }
      Iterator( (idx, attrs) )
    }, preservesPartitioning = true)
    val adjacencies = graph.adjacencies.mapPartitionsWithIndex({ (idx, it) =>
      val adjs = new Int2ObjectOpenHashMap[Adjacency]()
      it.foreach { case (v, adjInfo) => adjs.put(v, adjInfo) }
      Iterator( (idx, adjs) )
    }, preservesPartitioning = true)
    val nodesInfo = attributes.join(adjacencies).setName("nodesInfo").persist()

    val msgs = nodesInfo.mapPartitionsWithIndex { (idx, it) =>
      val toSend = Array.fill(partitioner.numPartitions)(new Int2ObjectOpenHashMap[V]())
      it.foreach { case (pid, (attrs, adjs)) =>
        require(pid == idx)
        val nodeIdIt = adjs.keySet().iterator()
        while(nodeIdIt.hasNext) {
          val node = nodeIdIt.nextInt()
          val adj = adjs.get(node)
          0 until adj.size foreach { i =>
            val dest = adj.neighbours(i)
            if (node < dest) {
              val partDest = partitioner.getPartition(dest)
              toSend(partDest).put(node, attrs.get(node))
            }
          }
        }
      }
      toSend.indices.iterator.map { i =>
        (i, toSend(i))
      }
    }.reduceByKey { (a, b) =>
      a.putAll(b)
      a
    }

    nodesInfo.join(msgs).mapPartitionsWithIndex({(idx, it) =>
      val edges = mutable.Map[EdgeId, Distance]()
      it.foreach { case (pid, ((attrs, adjs), nodes)) =>
        require(pid == idx)
        val nodeIt = attrs.keySet().iterator()
        while(nodeIt.hasNext) {
          val srcId = nodeIt.nextInt()
          val srcAttr = attrs.get(srcId)
          for(i <- adjs.get(srcId).neighbours.indices) {
            val dstId = adjs.get(srcId).neighbours(i)
            val dstAttr = nodes.get(dstId)
            if (srcId > dstId && srcAttr.center != dstAttr.center) {
              val edgeId =
                if (srcAttr.center < dstAttr.center) (srcAttr.center, dstAttr.center)
                else (dstAttr.center, srcAttr.center)
              val newWeight = srcAttr.distance + dstAttr.distance + adjs.get(srcId).weights(i)
              edges.get(edgeId) match {
                case None => edges.put(edgeId, newWeight)
                case Some(oldWeight) =>
                  if(newWeight < oldWeight) {
                    edges.put(edgeId, newWeight)
                  }
              }
            }
          }
        }
      }
      edges.iterator
    }).reduceByKey(math.min)

  }

  def buildEdges[V <: CenterInfo with DistanceInfo](graph: EdgeGraph[V])
                                                   (implicit tag: ClassTag[V])
  : RDD[(EdgeId, Distance)] = action("Edge construction from edge graph") {

    val messages = graph.nodes.join(graph.edges).map { case (id, (v, (dst, w))) =>
      (dst, Msg(v.center, v.distance + w))
    }

    graph.nodes.join(messages).map { case (id, (v, msg)) =>
      ((v.center, msg.center), v.distance + msg.distance)
    }.reduceByKey(math.min)
      .persist(persistLevel).setName("Edges")

  }

}

private case class Msg(center: NodeId, distance: Distance)

