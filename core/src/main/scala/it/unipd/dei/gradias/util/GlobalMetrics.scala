package it.unipd.dei.gradias.util

import org.apache.spark.Success
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}
import scala.collection.mutable
import scala.reflect.runtime.universe._

/**
 * Collects various Spark metrics. The methods of this class are called
 * by thread `SparkListenerBus`, so there's no need to create a new thread
 * in order not to block the main computation. Besides, the calls are not
 * concurrent.
 */
class GlobalMetrics extends SparkListener {
  
  private val stagesMetrics = mutable.Map[Int, AggregateMetric]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = taskEnd.reason match {
    case Success =>
      val old = stagesMetrics.getOrElse(taskEnd.stageId, AggregateMetric())
      stagesMetrics.update(taskEnd.stageId, old + taskEnd.taskMetrics)
    case _ => // do nothing for non-successful tasks
  }

  def stages: Map[Int, AggregateMetric] = stagesMetrics.toMap
  def aggregate: AggregateMetric = stages.values.reduce(_ + _)

}

case class AggregateMetric(
                            totalParallelTime: Long = 0L,
                            jvmGCTime: Long = 0L,
                            shuffleWriteTime: Long = 0L,
                            shuffleWriteBytes: Long = 0L,
                            shuffleRemoteReadBytes: Long = 0L,
                            fetchWaitTime: Long = 0L,
                            diskSpilledBytes: Long = 0L) {

  val jvmGCfrac: Double = jvmGCTime.toDouble / totalParallelTime

  def + (other: AggregateMetric): AggregateMetric =
    AggregateMetric(
      totalParallelTime = totalParallelTime + other.totalParallelTime,
      jvmGCTime = jvmGCTime + other.jvmGCTime,
      fetchWaitTime = fetchWaitTime + other.fetchWaitTime,
      shuffleRemoteReadBytes = shuffleRemoteReadBytes + other.shuffleRemoteReadBytes,
      shuffleWriteBytes = shuffleWriteBytes + other.shuffleWriteBytes,
      shuffleWriteTime = shuffleWriteTime + other.shuffleWriteTime,
      diskSpilledBytes = diskSpilledBytes + other.diskSpilledBytes
    )

  def + (task: TaskMetrics): AggregateMetric = this + AggregateMetric.fromTask(task)

  def toMap: Map[String, AnyVal] = {
    val typeMirror = runtimeMirror(this.getClass.getClassLoader)
    val iMirror = typeMirror.reflect(this)
    val fields = iMirror.symbol.typeSignature.members.map(s => s.asTerm).filter(_.isVal)
    val kvPairs =
      fields.map { f =>
        val fMirror = iMirror.reflectField(f)
        (f.name.decoded.trim, fMirror.get.asInstanceOf[AnyVal])
      }
    kvPairs.toMap
  }

}

object AggregateMetric {

  def fromTask(task: TaskMetrics): AggregateMetric =
    AggregateMetric(
      totalParallelTime = task.executorRunTime + task.jvmGCTime + task.resultSerializationTime + task.executorDeserializeTime,
      jvmGCTime = task.jvmGCTime,
      fetchWaitTime = task.shuffleReadMetrics.map(_.fetchWaitTime).getOrElse(0L),
      shuffleRemoteReadBytes = task.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L),
      shuffleWriteBytes = task.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L),
      shuffleWriteTime = task.shuffleWriteMetrics.map(_.shuffleWriteTime).getOrElse(0L) / 1000000,
      diskSpilledBytes = task.diskBytesSpilled
    )

}