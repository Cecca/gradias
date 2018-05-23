package it.unipd.dei.gradias.util

import it.unimi.dsi.logging.ProgressLogger
import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

import scala.collection.mutable

object ProgressLoggerListener {

  val logger = LoggerFactory.getLogger("progress")

}

class ProgressLoggerListener extends SparkListener {

  val progressLoggers = mutable.Map[Int, ProgressLogger]()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val sid = stageSubmitted.stageInfo.stageId
    val pl = new ProgressLogger(ProgressLoggerListener.logger)
    progressLoggers(sid) = pl
    pl.start()
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val sid = stageCompleted.stageInfo.stageId
    val pl = progressLoggers(sid)
    if(pl.count != 0) {
      pl.updateAndDisplay(0)
    }
    pl.stop()
    progressLoggers.remove(sid)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    taskEnd.taskMetrics.inputMetrics.foreach { metrics =>
      val sid = taskEnd.stageId
      val pl = progressLoggers(sid)
      pl.update(metrics.recordsRead)
    }
  }
}
