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

package it.unipd.dei.gradias.util

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

object Logger {

  val algoLogger = LoggerFactory.getLogger("algorithm")

  val toolLogger = LoggerFactory.getLogger("tool")

  val timerLogger = LoggerFactory.getLogger("timer")

  def red(s: Any) = Console.RED + s + Console.RESET
  def yellow(s: Any) = Console.YELLOW + s + Console.RESET
  def green(s: Any) = Console.GREEN + s + Console.RESET
  def blue(s: Any) = Console.BLUE + s + Console.RESET
  def cyan(s: Any) = Console.CYAN + s + Console.RESET
  def magenta(s: Any) = Console.MAGENTA + s + Console.RESET
  def white(s: Any) = Console.WHITE + s + Console.RESET
  def black(s: Any) = Console.BLACK + s + Console.RESET
  def underlined(s: Any) = Console.UNDERLINED + s + Console.RESET

  def logRDDs(sc: SparkContext): Unit = {
    val strs = sc.getPersistentRDDs.toSeq.sortBy(_._1).map { case (id, rdd) =>
      s"$id: ${rdd.name}"
    }
    algoLogger.info("Persistent RDDs")
    for(s <- strs) {
      algoLogger.info("  " + s)
    }
  }

}
