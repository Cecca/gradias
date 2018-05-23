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

import it.unipd.dei.gradias.diameter.FloydWarshallTryAlternativesBench
import it.unipd.dei.gradias.diameter.dijkstra.{DijkstraBench, DijkstraRegressionBench}
import it.unipd.dei.gradias.weighted.VertexImplementationsBench

object Benchmark {

  val benchmarks = Map(
    "Dijkstra" -> {args: Array[String] => DijkstraBench.main(args)},
    "DijkstraRegression" -> {args: Array[String] => DijkstraRegressionBench.main(args)},
    "FloydWarshallAlternativeImpls" -> {args: Array[String] => FloydWarshallTryAlternativesBench.main(args)},
    "Vertex" -> {args: Array[String] => VertexImplementationsBench.main(args)}
  )

  def main(args: Array[String]) {

    args.toList match {

      case List("--help") =>
        println("Available benchmarks\n")
        for((name, _) <- benchmarks) {
          println(s"    $name")
        }

      case name :: args if benchmarks.contains(name) =>
        println(s"===== $name ==================================")
        println(s"Aruments are $args")
        benchmarks.get(name).get.apply(args.toArray)

      case args =>
        println("Running all benchmarks")
        for((name, fn) <- benchmarks) {
          println(s"===== $name ==================================")
          fn(args.toArray)
        }
    }

  }

}
