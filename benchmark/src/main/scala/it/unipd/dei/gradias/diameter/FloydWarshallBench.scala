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

package it.unipd.dei.gradias.diameter

import org.scalameter.Key._
import org.scalameter._

object FloydWarshallTryAlternativesBench extends PerformanceTest.OfflineReport {

  val sizes = Gen.exponential("size")(128, 1024, 2)
  val matrices = for (sz <- sizes) yield MatrixHelpers.symmetricZeroDiagonalRaw(sz)

  performance of "Matrix" config (exec.independentSamples -> 1) in {

    measure method "floydWarshall" in {

      using(matrices) curve "baseline" in { m =>
        FloydWarshallAlternativeImpls.baseline(m)
      }

      using(matrices) curve "half-matrix" in { m =>
        FloydWarshallAlternativeImpls.half(m)
      }
    }

  }

}
