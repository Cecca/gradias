package it.unipd.dei.gradias.weighted.decomposition

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.Timer.{action, timed}
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.util.ExperimentUtil.jMap
import it.unipd.dei.gradias.util.Logger._
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.{Bucket, _}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.util.Random

object DeltaCluster {

  val persistLevel = Utils.storageLevel

  case class Vertex(
                     center: NodeId,
                     phaseDistance: Distance,
                     offsetDistance: Distance,
                     flags: Byte)
    extends UpdatableVertex with CenterInfo with DistanceInfo with BucketInfo
    with QuotientInfo with CoveringInfo {

    override val bucketDistance: Distance = phaseDistance

    def distance: Distance = Distance.sum(offsetDistance, phaseDistance)

    def isUncovered: Boolean = !covered

    def isQuotient: Boolean = isUncovered || isCenter

    def isStable: Boolean = offsetDistance != 0.0f || isCenter

    def updated: Boolean = (flags & Vertex.UpdatedMask) == Vertex.UpdatedMask

    def covered: Boolean = (flags & Vertex.CoveredMask) == Vertex.CoveredMask

    override def isCenter: Boolean = distance == 0.0f

    def updateWith(message: Message, bucket: Bucket): Vertex = {
      var newFlags = Vertex.setUpdated(flags)
      if (message.phaseDistance < bucket.maxDistance){
        newFlags = Vertex.setCovered(newFlags)
      }
      this.copy(
        center = message.center,
        phaseDistance = message.phaseDistance,
        offsetDistance = message.offsetDistance,
        flags = newFlags)
    }

  }

  object Vertex {

    val UpdatedMask: Byte = 1
    val CoveredMask: Byte = 2

    def setUpdated(flags: Byte): Byte = (flags | UpdatedMask).toByte
    def setCovered(flags: Byte): Byte = (flags | CoveredMask).toByte
    def unsetUpdated(flags: Byte): Byte = (flags & ~UpdatedMask).toByte
    def unsetCovered(flags: Byte): Byte = (flags & ~CoveredMask).toByte

    def apply(): Vertex = new Vertex(
      center = -1,
      phaseDistance = Infinity,
      offsetDistance = 0.0f,
      flags = 0)

    def makeCenter(id: NodeId, v: Vertex) =
      v.copy(center = id, phaseDistance = 0.0f, offsetDistance = 0.0f, flags = 3)

  }

  implicit def graphToDeltaGraphOps(graph: Graph[Vertex]): DeltaGraphOps =
    new DeltaGraphOps(graph)

  class DeltaGraphOps(graph: Graph[Vertex]) {

    def completeCovering(): Graph[Vertex] =
      graph.mapNodes { case (id, vertex) =>
        if (vertex.isUncovered)
          Vertex.makeCenter(id, vertex)
        else
          vertex
      }

    def selectCenters(prob: Double): Graph[Vertex] =
      graph.mapNodes { case (id, v) =>
        if (v.isUncovered && Random.nextDouble() <= prob)
          Vertex.makeCenter(id, v)
        else
          v
      }

    def resetNodes(): Graph[Vertex] =
      graph.mapNodes { (id, v) =>
        if (v.isUncovered)
          Vertex()
        else
          v.copy(offsetDistance = v.distance, phaseDistance = 0, flags = Vertex.setUpdated(v.flags))
      }

    def activateCoveredNodes(): Graph[Vertex] =
      graph.mapNodes { (id, v) =>
        if (v.isCovered)
          v.copy(flags = Vertex.setUpdated(v.flags))
        else
          v
      }


  }

  case class Message(center: NodeId, phaseDistance: Distance, offsetDistance: Distance) {

    def distance: Distance = Distance.sum(phaseDistance, offsetDistance)

  }

  object Message {

    def apply(
               v: Vertex,
               edgeWeight: Distance): Message =
      new Message(v.center, edgeWeight + v.phaseDistance, v.offsetDistance)

    def min(a: Message, b: Message): Message = {
      if (a.distance < b.distance)
        a
      else b
    }

  }

  def extractResult(graph: Graph[Vertex]): RDD[(NodeId, DsdResultVertex)] = {
    val neighbourhoods = graph.neighbourhoods()

    graph.nodes.join(neighbourhoods).mapValues { case (v, ns) =>
      DsdResultVertex(center = v.center, distance = v.distance, neighbourhood = ns)
    }
  }

  case class DsdResultVertex(
                              center: NodeId,
                              distance: Distance,
                              neighbourhood: Array[NodeId])
    extends CenterInfo with DistanceInfo with NeighbourhoodInfo {

    override def isCenter: Boolean = distance == 0.0f
  }

  def init(graph: RDD[(EdgeId, Distance)], graphImpl: String): Graph[Vertex] = action("Initialization") {
    Graph.fromEdges(graph, Vertex.apply, graphImpl).setName("Initial graph").persist(persistLevel)
  }

}

class DeltaCluster(@transient val experiment: Experiment) {
  import DeltaCluster._

  var globalRelaxationIndex: Int = 0

  def decomposition(
                     raw: RDD[(EdgeId, Distance)],
                     target: Long,
                     delta: Distance,
                     refinementStrategy: RefinementStrategy,
                     steps: Int,
                     graphImpl: String): (Graph[Vertex], Distance) = {

    val graph = init(raw, graphImpl)
    decomposition(graph, target, delta, refinementStrategy, steps, graphImpl)
  }

  def decomposition(
                     graph: Graph[Vertex],
                     target: Long,
                     delta: Distance,
                     refinementStrategy: RefinementStrategy,
                     steps: Int,
                     graphImpl: String): (Graph[Vertex], Distance) = {
    globalRelaxationIndex = 0

    val centerSeq = Utils.centersSeq(graph.numNodes, target)

    val result = timed("clustering") {
      val (_res, _delta) = phases(graph, target, delta, refinementStrategy, steps, centerSeq)

      experiment.tag("delta", _delta)

      val completed = action("complete covering") {
        _res.completeCovering()
          .setName("Result")
          .persist(persistLevel)
      }

      _res.unpersistNodes(blocking = false)
      (completed, _delta)
    }

    result
  }

  @tailrec
  final def phases(
              graph: Graph[Vertex],
              target: Long,
              delta: Distance,
              refinementStrategy: RefinementStrategy,
              steps: Int,
              centersSeq: Seq[Double],
              phaseIndex: Int = 0)
  : (Graph[Vertex], Distance) = {

    val uncoveredAcc = graph.sparkContext.accumulator(0L)
    val centerAcc = graph.sparkContext.accumulator(0L)
    graph.mapNodes { case (_, v) =>
      if (v.isCenter)
        centerAcc += 1
      if (v.isUncovered)
        uncoveredAcc += 1
      0
    }.numNodes

    val numUncovered = uncoveredAcc.value
    val numCenters = centerAcc.value
    val quotientSize = numCenters + numUncovered

    if (quotientSize < target) {
      algoLogger.info(
        s"Quotient size (${yellow(quotientSize)}) is below the target ${yellow(target)}.")
      return (graph, delta)
    }
    if (centersSeq.isEmpty){
      algoLogger.info("There are no more centers, to select, stopping")
      return (graph, delta)
    }

    algoLogger.info(s"${red("="*20)} Start phase $phaseIndex ${red("="*20)}")
    algoLogger.info(s"( ${green(target)} | ${red(numUncovered)} | ${yellow(numCenters)} ) "+
      "(target | uncovered | centers)")

    val wCenters = action("center selection") {
      graph.resetNodes()
        .selectCenters(centersSeq.head / numUncovered)
        .setName("Graph with centers")
        .persist(persistLevel)
    }
    graph.unpersistNodes()

    val (updated, newDelta, ellDelta) = timed(s"Phase $phaseIndex") {
      doTentatives(wCenters, math.max(numUncovered / 2, target), delta, refinementStrategy, steps, 0)
    }
    wCenters.unpersistNodes() // TODO this unpersist call can probably be removed

    experiment.append("phases",
      jMap(
        "phase" -> phaseIndex,
        "time" -> Timer.getMillis(s"Phase $phaseIndex"),
        "l-delta" -> ellDelta))

    phases(updated, target, newDelta, refinementStrategy, steps, centersSeq.tail, phaseIndex + 1)
  }

  def doTentatives(
                    graph: Graph[Vertex],
                    phaseTarget: Long,
                    delta: Distance,
                    refinementStrategy: RefinementStrategy,
                    steps: Int,
                    tentativeIdx: Int)
  : (Graph[Vertex], Distance, Int) = {
    val tName = s"Tentative $tentativeIdx (Δ=$delta)"
    algoLogger.info(s"${yellow("*"*20)} $tName ${yellow("*"*20)}")
    timed(tName) {
      doPhase(graph, phaseTarget, delta, refinementStrategy, steps, 0)
    } match {
      case Right((g, ellDelta)) =>
        (g, delta, ellDelta)
      case Left(g) =>
        algoLogger.info(s"Tentative $tentativeIdx (Δ=$delta) failed, retrying")
        val newGraph = action("Activate covered nodes") {
          g.activateCoveredNodes().setName("Intermediate tentative").persist(persistLevel)
        }
        g.unpersistNodes(blocking = false)
        doTentatives(newGraph, phaseTarget, 2*delta, refinementStrategy, steps, tentativeIdx+1)
    }
  }


  @tailrec
  final def doPhase(
               graph: Graph[Vertex],
               phaseTarget: Long,
               delta: Distance,
               refinementStrategy: RefinementStrategy,
               steps: Int,
               bucketIndex: Int)
  : Either[Graph[Vertex], (Graph[Vertex], Int)] = {
    if (bucketIndex >= steps) {
      return Left(graph)
    }

    val bucket = Bucket(delta, index = bucketIndex)

    algoLogger.info(s"Exploring bucket $bucket")

    val (updatedGraph, ellDelta, quotientSize) = deltaStep(
      bucket, graph,
      phaseTarget,
      refinementStrategy,
      iteration = 0)

    if (quotientSize > phaseTarget) {
      doPhase(updatedGraph, phaseTarget, delta, refinementStrategy, steps, bucketIndex + 1)
    } else {
      Right((updatedGraph, ellDelta))
    }
  }

  @tailrec
  final def deltaStep(
                 bucket: Bucket,
                 graph: Graph[Vertex],
                 phaseTarget: Long,
                 refinementStrategy: RefinementStrategy,
                 iteration: Int): (Graph[Vertex], Int, Long) = { // graph, ellDelta, quotientSize

    val updatedCounter = graph.sparkContext.accumulator(0L)
    val quotientCounter = graph.sparkContext.accumulator(0L)
    val messageCounter = graph.sparkContext.accumulator(0L)

    val start = System.currentTimeMillis()
    val updated = lightRelaxation(bucket, graph, updatedCounter, quotientCounter, messageCounter)
    updated.numNodes
    val updatedCnt = updatedCounter.value
    val quotientSize = quotientCounter.value
    val end = System.currentTimeMillis()

    graph.unpersistNodes(blocking = false)

    algoLogger.info(s"Light step updated ${yellow(updatedCnt)} nodes in bucket ${bucket.index}")
    algoLogger.info(s"Quotient size: ${yellow(quotientSize)} phase target:  ${yellow(phaseTarget)}")
    experiment.append("relaxations",
      jMap(
        "idx" -> globalRelaxationIndex,
        "iteration" -> iteration,
        "type" -> "light",
        "updateRequests" -> messageCounter.value,
        "updatedNodes" -> updatedCnt,
        "time" -> (end - start)))
    globalRelaxationIndex += 1

    if (updatedCnt > 0) {

      val strategy = refinementStrategy.init(iteration+1)

      if (quotientSize <= phaseTarget && strategy.shouldContinue) {
        algoLogger.info(s"Below the target, but $strategy imposes to continue")
        deltaStep(bucket, updated, phaseTarget, strategy.next, iteration+1)
      } else if(quotientSize <= phaseTarget && strategy.shouldStop) {
        algoLogger.info(s"Below the target, and strategy $strategy tells to stop")
        (updated, iteration+1, quotientSize)
      } else {
        deltaStep(bucket, updated, phaseTarget, refinementStrategy, iteration+1)
      }
    } else {
      algoLogger.info("No nodes updated, finish delta-step")
      (updated, iteration+1, quotientSize)
    }
  }

  def lightRelaxation(bucket: Bucket,
                      graph: Graph[Vertex],
                      updatedCounter: Accumulator[Long],
                      quotientCounter: Accumulator[Long],
                      messageCounter: Accumulator[Long])
  : Graph[Vertex] = {

    val updated = graph.exchangeMessages[Vertex, Message](
      shouldSend = { (id, v) => v.updated && bucket.contains(v) },
      shouldTransmit = { (src, dst, w) => bucket.isLight(w) },
      makeMessage = { (id, v, dest, weight) => Message(v, weight) },
      mergeMessages = Message.min,
      joinFunc = {
        case (vertex, Some(message))
          if vertex.isUncovered || (message.distance < vertex.distance && !vertex.isStable) =>
          updatedCounter += 1
          val up = vertex.updateWith(message, bucket)
          if(up.isQuotient)
            quotientCounter += 1
          up
        case (vertex, _) if bucket.contains(vertex) =>
          if(vertex.isQuotient)
            quotientCounter += 1
          vertex.copy(flags = Vertex.unsetUpdated(vertex.flags))
        case (vertex, _) =>
          if(vertex.isQuotient)
            quotientCounter += 1
          vertex
      },
      messageCounter
    ).setName(s"light relaxation (bucket ${bucket.index})").persist(persistLevel)

    updated.checkpointEvery(Utils.checkpointInterval)

    updated
  }

  @deprecated("Heavy relaxation is no-longer required", "0.11.0")
  def heavyRelaxation(bucket: Bucket, graph: Graph[Vertex]): Graph[Vertex] = {

    val updated = graph.exchangeMessages[Vertex, Message](
      shouldSend = { (id, v) => bucket.contains(v) },
      shouldTransmit = { (src, edge, w) => !bucket.isLight(w) },
      makeMessage = { (id, v, dest, weight) => Message(v, weight) },
      mergeMessages = Message.min,
      joinFunc = {
        case (vertex, Some(message))
          if vertex.isUncovered || (message.distance < vertex.distance && !vertex.isStable) =>
          vertex.updateWith(message, bucket)
        case (vertex, _) if bucket.contains(vertex) => vertex.copy(flags = Vertex.unsetUpdated(vertex.flags))
        case (vertex, _) => vertex
      },
      ???
    ).setName(s"heavy relaxation (bucket ${bucket.index})").persist(persistLevel)

    updated.checkpointEvery(Utils.checkpointInterval)

    updated
  }

}