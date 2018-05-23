package it.unipd.dei.gradias

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import it.unipd.dei.gradias.util.Logger.algoLogger

object UnweightedGraphLoader {

   def loadGraph(sc: SparkContext, input: String): RDD[(NodeId, Array[NodeId])] = {
     algoLogger.info("Loading file {}", input)
     sc.textFile(input).map({line =>
       val dat = line.split("\\s+")
       (dat.head.toInt, dat.tail.map(_.toInt).sorted.toArray)
     })
   }

 }
