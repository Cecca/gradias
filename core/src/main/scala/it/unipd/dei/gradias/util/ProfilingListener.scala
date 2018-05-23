package it.unipd.dei.gradias.util


import it.unipd.dei.gradias.util.Logger._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListener, SparkListenerTaskEnd}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object ProfilingListener {

  val logger = LoggerFactory.getLogger("profiling")

}

class ProfilingListener extends SparkListener {
  import ProfilingListener._

  val stats = mutable.Map[Int, ProfileInfo]()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    stats(stageSubmitted.stageInfo.stageId) = new ProfileInfo()
    logger.info(s"${yellow("Start stage")} ${stageSubmitted.stageInfo.name}")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val prof = stats(stageCompleted.stageInfo.stageId)
    logger.info(prof.logString)
    logger.info(s"${green("Completed stage")} ${stageCompleted.stageInfo.name}")
    stats.remove(stageCompleted.stageInfo.stageId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    stats(taskEnd.stageId).update(taskEnd.stageId, taskEnd.taskMetrics)
  }
}

class ProfileInfo {

  var stageId: Option[Int] = None
  val executorRunTime: RunningStats = new RunningStats()
  val executorDeserializeTime: RunningStats = new RunningStats()
  val resultSerializationTime: RunningStats = new RunningStats()
  val jvmGCTime: RunningStats = new RunningStats()
  val shuffleWriteTime: RunningStats = new RunningStats()
  val fetchWaitTime: RunningStats = new RunningStats()

  def update(stage: Int, metrics: TaskMetrics): Unit = {
    stageId match {
      case None => stageId = Some(stage)
      case Some(id) => require(id == stage, "Profile info must refer to the same stage")
    }
    executorRunTime.push(metrics.executorRunTime)
    executorDeserializeTime.push(metrics.executorDeserializeTime)
    resultSerializationTime.push(metrics.resultSerializationTime)
    jvmGCTime.push(metrics.jvmGCTime)
    metrics.shuffleWriteMetrics.foreach { sm =>
      shuffleWriteTime.push(sm.shuffleWriteTime / 1000000) // this is in nanoseconds
    }
    metrics.shuffleReadMetrics.foreach { sm =>
      fetchWaitTime.push(sm.fetchWaitTime)
    }
  }

  def logString: String =
    s"Stage ${stageId.getOrElse(red("x"))}:" +
      s" run ${green(executorRunTime.mean.toLong)}±${cyan(executorRunTime.stddev.toLong)}" +
      s" deserialize ${green(executorDeserializeTime.mean.toLong)}±${cyan(executorDeserializeTime.stddev.toLong)}" +
      s" serialize ${green(resultSerializationTime.mean.toLong)}±${cyan(resultSerializationTime.stddev.toLong)}" +
      s" fetch ${green(fetchWaitTime.mean.toLong)}±${cyan(fetchWaitTime.stddev.toLong)}" +
      s" write ${green(shuffleWriteTime.mean.toLong)}±${cyan(shuffleWriteTime.stddev.toLong)}" +
      s" gc ${green(jvmGCTime.mean.toLong)}±${cyan(jvmGCTime.stddev.toLong)}"

}