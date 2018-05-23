package it.unipd.dei.gradias.weighted.decomposition

import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.{Distance, Infinity}
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.weighted.{BucketInfo, Bucket}
import org.apache.spark.graphx._

import scala.annotation.tailrec
import scala.util.Random

object GraphxCluster {

  def run[VD](graph: Graph[VD, Distance], target: Long, delta: Distance = 1)
  : Graph[GraphxClusterInfo, Distance] = {
    new GraphxCluster().run(graph, target, delta)
  }

}

class GraphxCluster {

  def run[VD](graph: Graph[VD, Distance], target: Long, delta: Distance = 1)
  : Graph[GraphxClusterInfo, Distance] = {

    val maxIterations = math.ceil(math.log(graph.ops.numVertices / target) / math.log(2))
    val batchDim = target / maxIterations

    algoLogger.info(s"Start clustering (iterations=$maxIterations, batch=$batchDim)")
    val clustered = cluster(init(graph), target, delta, batchDim)

    clustered
  }

  private def init[VD](graph: Graph[VD, Distance]): Graph[GraphxClusterInfo, Distance] =
    graph.mapVertices({ case _ => GraphxClusterInfo() }).groupEdges(math.min)

  @tailrec
  private def cluster(
                       graph: Graph[GraphxClusterInfo, Distance],
                       target: Long,
                       delta: Distance,
                       batchDim: Double)
  : Graph[GraphxClusterInfo, Distance] = {

    algoLogger.info("Start clustering a fraction of the graph")

    val quotientSize = graph.vertices.filter({ case (_, v) => v.isQuotient }).count()
    val numUncovered = graph.vertices.filter({ case (_, v) => !v.covered }).count()

    if (quotientSize < target) {
      return selectCenters(graph, 1.0)
    }

    // Add the next batch of centers
    val centerProb = batchDim / numUncovered
    val wCenters = selectCenters(graph, centerProb)

    val (updated, newDelta) = phase(wCenters, math.max(numUncovered / 2, target), delta)

    // Reset the nodes
    val reset = resetGraph(updated)

    cluster(reset, target, newDelta, batchDim)
  }

  private def resetGraph(updated: Graph[GraphxClusterInfo, Distance]): Graph[GraphxClusterInfo, Distance] = {
    updated.mapVertices { case (id, v) =>
      if (!v.covered) {
        GraphxClusterInfo()
      } else {
        v.copy(offsetDistance = v.distance, phaseDistance = 0, updated = true)
      }
    }
  }

  private def selectCenters(graph: Graph[GraphxClusterInfo, Distance], centerProb: Double) = {
    graph.mapVertices { case (id, v) =>
      if (!v.covered && Random.nextDouble() <= centerProb) {
        GraphxClusterInfo.makeCenter(id, v)
      } else {
        v
      }
    }
  }

  @tailrec
  private def phase(
                     graph: Graph[GraphxClusterInfo, Distance],
                     phaseTarget: Long,
                     delta: Distance)
  : (Graph[GraphxClusterInfo, Distance], Distance) = {

    tentative(graph, phaseTarget, delta) match {
      case Right(g) => (g, delta)
      case Left(g) =>
        val activated = g.mapVertices { case (_, v) =>
          if (v.covered) {
            v.copy(updated = true)
          } else {
            v
          }
        }
        phase(activated, phaseTarget, 2 * delta)
    }
  }

  private def tentative(
                         graph: Graph[GraphxClusterInfo, Distance],
                         phaseTarget: Long,
                         delta: Distance)
  : Either[Graph[GraphxClusterInfo, Distance], Graph[GraphxClusterInfo, Distance]] = {

    algoLogger.info(s"Doing a tentative with delta=$delta and target $phaseTarget")

    val bucket = Bucket(delta, 0)

    val updatedGraph = deltaStep(graph, bucket, phaseTarget)

    val quotientSize = updatedGraph.vertices.filter({ case (_, v) => v.isQuotient }).count()

    if (quotientSize <= phaseTarget) {
      Right(updatedGraph)
    } else {
      Left(updatedGraph)
    }
  }

  @tailrec
  private def deltaStep(
                         graph: Graph[GraphxClusterInfo, Distance],
                         bucket: Bucket,
                         phaseTarget: Long)
  : Graph[GraphxClusterInfo, Distance] = {

    val start = System.currentTimeMillis()

    val updated = relaxEdges(graph, bucket).persist(Utils.storageLevel)

    if(Random.nextDouble() < Utils.checkpointProbability) {
      algoLogger.info("Checkpointing")
      updated.checkpoint()
    }

    val updatedCnt = updated.vertices.filter({
      case (_, v) => v.updated && bucket.contains(v)
    }).count()
    val quotientSize = updated.vertices.filter({ case (_, v) => v.isQuotient }).count()

    updated.numVertices
    val end = System.currentTimeMillis()
    graph.unpersist(blocking = true)

    algoLogger.info(s"Delta step: ${end - start}ms elapsed ")

    if (updatedCnt == 0 || quotientSize <= phaseTarget) {
      updated
    } else {
      deltaStep(updated, bucket, phaseTarget)
    }
  }

  private def relaxEdges(graph: Graph[GraphxClusterInfo, Distance], bucket: Bucket)
  : Graph[GraphxClusterInfo, Distance] = {

    val messages = graph
      .subgraph(epred = { ctx => bucket.isLight(ctx.attr) })
      .aggregateMessages[GraphxClusterMessage](
        ctx => {
          if (ctx.srcAttr.updated && bucket.contains(ctx.srcAttr)) {
            ctx.sendToDst(GraphxClusterMessage(ctx.srcAttr, ctx.attr))
          }
          if (ctx.dstAttr.updated && bucket.contains(ctx.dstAttr)) {
            ctx.sendToSrc(GraphxClusterMessage(ctx.dstAttr, ctx.attr))
          }
        },
        GraphxClusterMessage.min,
        TripletFields.All
      )

    graph.outerJoinVertices(messages) {
      case (id, v, Some(msg)) if !v.covered || (msg.distance < v.distance && v.isUnstable) =>
        v.updateWith(msg, bucket)
      case (_, v, _) if bucket.contains(v) => v.copy(updated = false)
      case (_, v, _) => v
    }
  }


}


case class GraphxClusterMessage(
                                 center: VertexId,
                                 phaseDistance: Distance,
                                 offsetDistance: Distance) {

  def distance: Distance = phaseDistance + offsetDistance

}

object GraphxClusterMessage {

  def apply(v: GraphxClusterInfo, edgeWeight: Distance): GraphxClusterMessage =
    new GraphxClusterMessage(v.center, edgeWeight + v.phaseDistance, v.offsetDistance)

  def min(a: GraphxClusterMessage, b: GraphxClusterMessage): GraphxClusterMessage = {
    if (a.distance < b.distance) {
      a
    } else {
      b
    }
  }

}

/**
 * Contains various information about the role of the vertex in the clustering.
 *
 * @param center the ID of the center that captured the node
 * @param phaseDistance
 * @param offsetDistance
 * @param updated
 * @param covered
 */
case class GraphxClusterInfo(
                              center: VertexId,
                              phaseDistance: Distance,
                              offsetDistance: Distance,
                              updated: Boolean,
                              covered: Boolean) extends BucketInfo {

  /**
   * The distance from the center. To get the center ID, use `center`.
   * @return the distance from the cluster's center.
   */
  def distance: Distance = phaseDistance + offsetDistance

  override def bucketDistance(): Distance = phaseDistance

  /**
   * `true` if the node is itself a center
   * @return true if the node is itself a center.
   */
  def isCenter: Boolean = distance == 0

  def isQuotient: Boolean = !covered || isCenter

  def isUnstable: Boolean = offsetDistance == 0 && !isCenter

  def updateWith(message: GraphxClusterMessage, bucket: Bucket): GraphxClusterInfo =
    this.copy(
      center = message.center,
      phaseDistance = message.phaseDistance,
      offsetDistance = message.offsetDistance,
      updated = true,
      covered = covered || message.phaseDistance < bucket.maxDistance)

}

object GraphxClusterInfo {

  def apply(): GraphxClusterInfo = new GraphxClusterInfo(
    center = -1L,
    phaseDistance = Infinity,
    offsetDistance = 0,
    updated = false,
    covered = false
  )

  def makeCenter(id: VertexId, v: GraphxClusterInfo): GraphxClusterInfo =
    v.copy(center = id, phaseDistance = 0, updated = true, covered = true)

}
