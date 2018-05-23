package it.unipd.dei.gradias.util

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListener}

class JobKillerListener(val sparkContext: SparkContext) extends SparkListener {

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
    stageCompleted.stageInfo.failureReason match {
      case None => // do nothing
      case Some(reason) =>
        Logger.algoLogger.error(Logger.red(s"Job failed: $reason"))
        sparkContext.cancelAllJobs()
        sparkContext.stop()
    }
}
