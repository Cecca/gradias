package it.unipd.dei.gradias.weighted

import it.unipd.dei.gradias.{Distance, EdgeId}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Utilities to symmetrize and de-symmetrize graphs
 */
object SymmetryUtils {

  def symmetrize(graph: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] =
    graph.flatMap { case ((u, v), w) =>
      Seq(
        ((u, v), w),
        ((v, u), w)
      )
    }//.reduceByKey(math.min) // Reducing is not necessary, since there should not be duplicate edges

  def desymmetrize(graph: RDD[(EdgeId, Distance)])
  : RDD[(EdgeId, Distance)] =
    graph.map { case ((u, v), w) =>
      if (u < v)
        ((u, v), w)
      else
        ((v, u), w)
    }.reduceByKey(math.min)

  implicit def graphToSymmetryUtils(graph: RDD[(EdgeId, Distance)]): SymmetryUtils =
    new SymmetryUtils(graph)

}

class SymmetryUtils(rdd: RDD[(EdgeId, Distance)]) {

  def symmetrize(): RDD[(EdgeId, Distance)] = SymmetryUtils.symmetrize(rdd)
  def desymmetrize(): RDD[(EdgeId, Distance)] = SymmetryUtils.desymmetrize(rdd)

}
