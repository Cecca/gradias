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

import it.unipd.dei.gradias.diameter.MatrixHelpers._
import org.scalatest.FreeSpec

class FloydWarshallAlternativeImplsSpec extends FreeSpec {
  import it.unipd.dei.gradias.diameter.FloydWarshallAlternativeImpls._

  def doTest(dim: Int) = {
    val data = symmetricZeroDiagonalRaw(dim)

    val actual = new Matrix(half(deepClone(data)))

    val expected = new Matrix(baseline(deepClone(data)))

    assert(expected.equals(actual),
      s"------------------------------------------- expected: \n" +
        s"$expected\n" +
        s"------------------------------------------- actual: \n" +
        s"$actual")
  }

  "All the alternative implementations must be equals to the baseline" - {

    "half-matrix alternative 1" in { doTest(1) }
    "half-matrix alternative 2" in { doTest(2) }
    "half-matrix alternative 4" in { doTest(4) }
    "half-matrix alternative 8" in { doTest(8) }
    "half-matrix alternative 16" in { doTest(16) }
    "half-matrix alternative 32" in { doTest(32) }
    "half-matrix alternative 64" in { doTest(64) }
    "half-matrix alternative 128" in { doTest(128) }

  }

}
