package it.unipd.dei.gradias

import it.unipd.dei.gradias.Timer.action
import it.unipd.dei.gradias.util.Logger.{algoLogger, yellow}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class MessagingProtocol[V: ClassTag, M: ClassTag](
                                                   val genMessages: (NodeId, V) => TraversableOnce[(NodeId, M)],
                                                   val mergeMessages: (M, M) => M,
                                                   val joinMessages: (V, Option[M]) => V,
                                                   val persistLevel: StorageLevel)
  extends Serializable {

  def run(
           graph: RDD[(NodeId, V)],
           checkpoint: Boolean = false,
           actionName: String = "message exchange")
  : RDD[(NodeId, V)] = action(actionName) {

    val messages =
      graph
        .flatMap(x => genMessages(x._1, x._2))
        .reduceByKey(mergeMessages)

    require(graph.partitioner == messages.partitioner,
      "For efficiency, nodes and messages should be partitioned in the same way." +
        s"nodes: ${graph.partitioner}, messages ${messages.partitioner}")

    val updatedGraph =
      graph.leftOuterJoin(messages).mapValues { x =>
        joinMessages(x._1, x._2)
      }.setName("Graph").persist(persistLevel)

    if (checkpoint) {
      algoLogger.info(yellow("___checkpoint___"))
      updatedGraph.checkpoint()
    }

    updatedGraph
  }

}
