package it.unipd.dei.gradias.weighted.heuristic

import it.unipd.dei.experiment.Experiment
import it.unipd.dei.gradias._
import it.unipd.dei.gradias.weighted.decomposition.DeltaCluster

trait Heuristic {
  def run(graph: Graph[DeltaCluster.Vertex], delta: Distance, experiment: Experiment): Graph[DeltaCluster.Vertex]
}
