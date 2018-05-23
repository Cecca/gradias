package it.unipd.dei.gradias.weighted.heuristic

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias.util.Utils
import it.unipd.dei.gradias.weighted.decomposition.DeltaCluster.Vertex
import it.unipd.dei.gradias.weighted.decomposition.{DeltaCluster, Stop}
import it.unipd.dei.gradias.{Distance, Graph}

import scala.util.Random

/**
 * This heuristic selects, for each cluster, the farthest node from the
 * center of the cluster as a new center. Then, the graph is reset and a
 * new clustering is computed from these centers, hopefully leading to an
 * improved diameter estimation.
 *
 * There are a couple of caveats:
 *
 *  - There can be more than one node at maximum distance from a given
 *    center. Ties should be broken at random.
 *  - There are a lot of zero-radius clusters, that is, clusters made of
 *    a single node. In this case we have two possibilities:
 *
 *     1. select only a few of them as cluster centers
 *     2. keep them all as cluster centers, meaning that that zone of the
 *        graph will start already covered
 *
 *    The second strategy is more for the following reasons:
 *
 *     a. zero-radius clusters appear in sparsely connected regions of the
 *        graph that are slow to explore
 *     b. part of the graph starts as already explored
 */
class FarthestCenterHeuristic extends Heuristic {

  override def run(graph: Graph[DeltaCluster.Vertex], delta: Distance, experiment: Experiment)
  : Graph[DeltaCluster.Vertex] = {
    val newCenters = graph.nodes.map { case (id, v) =>
      (v.center, (id, v.distance))
    }.reduceByKey { case (node1@(id1, dist1), node2@(id2, dist2)) =>
      if (dist1 > dist2)             node1
      else if (dist2 > dist1)        node2
      else if (Random.nextBoolean()) node1
      else                           node2
    }.map { case (center, (id, distance)) =>
      id
    }.collect().toSet
    // we collect everything to the driver because these nodes are few, so we can exploit a broadcast join
    val newCentersBroadcast = graph.sparkContext.broadcast(newCenters)

    val newGraph = graph.mapNodes { case (id, v) =>
      if(newCentersBroadcast.value.contains(id)) Vertex.makeCenter(id, Vertex())
      else Vertex()
    }.persist(Utils.storageLevel)

    // Do a new run of clustering on the new graph.
    //
    //  - doTentatives is used in order to skip the center selection phase
    //  - target is `newCenters.size` because we want the a graph with the
    //    exact number of centers of the previous one
    //  - we use Stop stopping condition because we already know the correct delta
    val (result, _, _) =
      new DeltaCluster(experiment).doTentatives(newGraph, newCenters.size, delta, Stop(), 1, 0)
    result
  }

}
