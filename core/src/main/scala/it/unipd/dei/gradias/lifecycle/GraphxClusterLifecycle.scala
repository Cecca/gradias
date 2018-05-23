package it.unipd.dei.gradias.lifecycle

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.{action, timed}
import it.unipd.dei.gradias.diameter.dijkstra.Dijkstra
import it.unipd.dei.gradias.diameter.{CSCLocalGraph, Matrix}
import it.unipd.dei.gradias.io.EdgeKey
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.decomposition.{GraphxCluster, GraphxClusterInfo}
import it.unipd.dei.gradias.{Distance, Timer}
import org.apache.hadoop.io.FloatWritable
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD

class GraphxClusterLifecycle(
                              override val sc: SparkContext,
                              input: String,
                              target: Long,
                              delta: Distance
                              )
extends Lifecycle with BasicInfo with GradiasInfo with SparkInfo {

  override val experiment: Experiment = new Experiment()

  override val algorithmName: String = "delta-cluster-graphx"

  override type T = Graph[Int, Distance]

  private var diameter: Distance = -1
  private var edges: Array[(VertexId, VertexId, Distance)] = null
  private var remappings: Long2LongOpenHashMap = null
  private var quotient: CSCLocalGraph = null
  private var radiuses: Array[Distance] = null
  private var radiusMatrix: Matrix = null
  private var distanceMatrix: Matrix = null

  override def loadInput(): T = action("Input loading") {
    val edges: RDD[Edge[Distance]] =
      sc.sequenceFile[EdgeKey, FloatWritable](input, minPartitions = sc.defaultParallelism)
        .map { case (edge, edgeWeight) =>
          if(edge.src < edge.dst) {
            Edge[Distance](edge.src, edge.dst, edgeWeight.get())
          } else {
            Edge[Distance](edge.dst, edge.src, edgeWeight.get())
          }
      }
    Graph.fromEdges(edges, -1, storageLevel, storageLevel)
      .partitionBy(PartitionStrategy.EdgePartition2D, sc.defaultParallelism)
      .persist(storageLevel)
  }

  override def run(input: T): Unit = timed("time") {
    val res = action("clustering") {
      GraphxCluster.run(input, target, delta)
    }
    timed("conversion") {
      edges = extractEdges(res)
      remappings = remapIds(edges)
      edges = edges.flatMap { edge =>
        Seq(
          (remappings.get(edge._1), remappings.get(edge._2), edge._3),
          (remappings.get(edge._2), remappings.get(edge._1), edge._3))
      }
      radiuses = buildRadiuses(res)
      quotient = CSCLocalGraph(edges.iterator)
    }
    timed("diameter") {
      radiusMatrix = buildRadiusMatrix(radiuses)
      distanceMatrix = new Matrix(Dijkstra.apsp(sc, quotient))
      diameter = radiusMatrix.sum(distanceMatrix).max
    }
  }

  override def updateMainTable(table: Map[String, Any]): Map[String, Any] = {
    super.updateMainTable(table) ++ Map(
      "diameter" -> diameter,
      "radius" -> radiuses.max,
      "nodes" -> quotient.n,
      "edges" -> quotient.nonZero / 2,
      "t_clustering" -> Timer.getMillis("clustering"),
      "t_conversion" -> Timer.getMillis("conversion"),
      "t_diameter" -> Timer.getMillis("diameter"),
      "time" -> Timer.getMillis("time")
    )
  }

  private def extractEdges(graph: Graph[GraphxClusterInfo, Distance]) = {
    graph.triplets.map { t =>
      ((t.srcAttr.center, t.dstAttr.center), t.srcAttr.distance + t.dstAttr.distance + t.attr)
    }.reduceByKey(math.min)
      .map({ case ((u, v), w) => (u, v, w) })
      .collect()
  }

  private def buildRadiuses(graph: Graph[GraphxClusterInfo, Distance]): Array[Distance] = {
    graph.vertices
      .map({case (_, v) => (v.center, v.distance)})
      .reduceByKey(math.max)
      .collect()
      .map(v => (remappings.get(v._1), v._2)).sorted.map(_._2)
  }

  private def buildRadiusMatrix(radiuses: Array[Distance]): Matrix = {
    val n = radiuses.length
    val data = Array.fill[Distance](n,n)(0)
    var i = 0
    while(i<n) {
      var j = 0
      while(j<n) {
        data(i)(j) = radiuses(i) + radiuses(j)
        j+=1
      }
      i+=1
    }
    val mat = new Matrix(data)
    assert(mat.isSymmetric)
    mat
  }

  def remapIds(edges: Iterable[(VertexId, VertexId, Distance)]): Long2LongOpenHashMap = {
    val m = new Long2LongOpenHashMap()
    var cnt = 0L

    edges.map { case (src, dst, weight) =>
      if(!m.containsKey(src)) {
        m.put(src, cnt)
        cnt += 1
      }
      if(!m.containsKey(dst)) {
        m.put(dst, cnt)
        cnt += 1
      }
      (m.get(src), m.get(dst), weight)
    }
    m
  }

}
