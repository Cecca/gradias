/*
 * gradias: distributed graph algorithms
 * Copyright (C) 2013-2015 Matteo Ceccarello
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package it.unipd.dei.gradias

import com.codahale.metrics.MetricRegistry
import it.unipd.dei.gradias.util.Logger._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Timer {

  private object Indentation {
    val indentBase: String = "."
    val indentWidth = 2
  }

  private case class NestedTimer(
                                  sequentialIndex: Int,
                                  nestingLevel: Int,
                                  name: String,
                                  time: Long)

  private class NestedTimerContext {
    var nestingLevel = 0
    var sequentialIndex = 0

    val timers = mutable.MutableList[NestedTimer]()

    def inc(): Unit = nestingLevel += 1
    def dec(): Unit = nestingLevel -= 1
    def advance(): Unit = sequentialIndex += 1

    def add(idx: Int, name: String, time: Long): Unit = {
      timers += NestedTimer(idx, nestingLevel, name, time)
    }

    def indentationStr: String =
      Indentation.indentBase + (Indentation.indentBase * Indentation.indentWidth * nestingLevel)

  }

  private val nestedTimers: NestedTimerContext = new NestedTimerContext()

  val registry = new MetricRegistry()

  val millisFactor  = 0.000001
  val secondsFactor = 0.000000001

  def getMillis(name: String): Int =
    (registry.timer(name).getSnapshot.getMean * millisFactor).toInt

  def getSeconds(name: String): Double =
    registry.timer(name).getSnapshot.getMean * secondsFactor

  def reportTimingPercentages() = {
    val timers = nestedTimers.timers.toList.sortBy(_.sequentialIndex)
    val totals = timers.groupBy(_.nestingLevel).mapValues(_.map(_.time).sum.toDouble)
    timerLogger.info("Total - " +
      green(f"${totals(0)*millisFactor}%.2f ms ")+
      "(" + yellow("100 %") + ")")
    for (NestedTimer(idx, level, name, time) <- timers) {
//      val levelTotal = timers.filter(_.nestingLevel == level).map(_.time).sum.toDouble
      val perc = time / totals(level) * 100
      timerLogger.info("." + "."*level*4 +
        f" $name%-20s - " +
        green(f"${time*millisFactor}%.2f ms ")+
        "(" + yellow(f"$perc%.2f %%") + ")")
    }
  }

  def timed[R](name: String)(f: => R): R = {
    val ctx = registry.timer(name).time()
    val ret = f
    val elapsed = ctx.stop()
    timerLogger.info("{} time: {}", name, elapsed*millisFactor)
    ret
  }
  
  def action[R](name: String)(f: => R): R = {
    val ctx = registry.timer(name).time()
    timerLogger.info(s"${nestedTimers.indentationStr} ${cyan("Starting")} ${magenta(name)}")
    val seqIdx = nestedTimers.sequentialIndex
    nestedTimers.advance()
    nestedTimers.inc()
    val ret = f
    val evalMsg = forceEvaluation(ret)
    val elapsed = ctx.stop()
    nestedTimers.dec()
    nestedTimers.add(seqIdx, name, elapsed)
    timerLogger.info(s"${nestedTimers.indentationStr} ${cyan("Finish")} $name: "+
      s"${underlined((elapsed*millisFactor).formatted("%.2f"))} ms - ${evalMsg}")
    ret
  }

  def forceEvaluation(data: Any): String = data match {
    case g: org.apache.spark.graphx.Graph[_,_] =>
      val verts = g.ops.numVertices
      s"$verts vertices"
    case g: Graph[_] =>
      val verts = g.numNodes
      s"graph with $verts nodes"
    case rdd: RDD[_] =>
      val cnt = rdd.count
      s"$cnt elements"
    case any =>
      timerLogger.warn(
        red(s"Don't know how to force evaluation of ${any.getClass}"))
      s"result = $any"
  }

}
