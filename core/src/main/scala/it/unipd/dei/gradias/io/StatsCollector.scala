package it.unipd.dei.gradias.io

import com.clearspring.analytics.stream.cardinality.HyperLogLog
import it.unipd.dei.gradias.util.{ProfilingListener, ProgressLoggerListener, RunningStats}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class PartialStat(edgeStats: RunningStats, maxId: Long, approxNodes: HyperLogLog) {
  def merge(other: PartialStat): PartialStat = {
    PartialStat(
      this.edgeStats.merge(other.edgeStats),
      math.max(this.maxId, other.maxId),
      this.approxNodes.merge(other.approxNodes).asInstanceOf[HyperLogLog]
    )
  }
}

object StatsCollector {

  val statsKeys = Set(
    "maxId",
    "approxNodes",
    "edges",
    "weightMean",
    "weightStddev",
    "weightMin",
    "weightMax")

  def collect(seqFile: String, sparkCtx: Option[SparkContext] = None): Map[String, AnyVal] = {
    val meta = Metadata.forFile(seqFile)

    // If metadata does not contain all the information we need,
    // compute it and update the metadata file
    if(!statsKeys.subsetOf(meta.toMap.keySet)) {
      val sc = sparkCtx match {
        case Some(_sc) => _sc
        case None =>
          val conf = new SparkConf (loadDefaults = true)
          conf
            .setAppName ("Dataset statistics")
            .set ("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set ("spark.kryo.registrator", "it.unipd.dei.gradias.serialization.GraphKryoRegistrator")
          val _sc = new SparkContext (conf)
          _sc.addSparkListener (new ProgressLoggerListener () )
          _sc.addSparkListener (new ProfilingListener () )
          _sc
      }

      val edges: RDD[(EdgeKey, Float)] = sc.sequenceFile(seqFile)
      val stats = edges.mapPartitions { it =>
        val eStats = new RunningStats()
        val approxNodes = new HyperLogLog(10)
        var maxId = 0L
        it.foreach { case (eKey, weight) =>
          eStats.push(weight)
          maxId = math.max(maxId, math.max(eKey.src, eKey.dst))
          approxNodes.offer(eKey.src)
          approxNodes.offer(eKey.dst)
        }
        Iterator(PartialStat(eStats, maxId, approxNodes))
      }.reduce(_ merge _)

      val mStats = Map(
        "maxId" -> stats.maxId,
        "approxNodes" -> stats.approxNodes.cardinality(),
        "edges" -> stats.edgeStats.count,
        "weightMean" -> stats.edgeStats.mean,
        "weightStddev" -> stats.edgeStats.stddev,
        "weightMin" -> stats.edgeStats.min,
        "weightMax" -> stats.edgeStats.max)

      meta.setAll(mStats)
      meta.save()
    }

    meta.toMap.map { case (k,v) =>
      (k, v.asInstanceOf[AnyVal])
    }
  }

}
