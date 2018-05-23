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

package it.unipd.dei.gradias.anf

import scala.util.Random

class ProbabilisticCounter(val registers: Array[Int]) extends Serializable {
  import it.unipd.dei.gradias.anf.ProbabilisticCounter._

  def estimate: Double = {
    val exponent = registers.map(lowestZeroBit).sum / registers.length
    println(s"exponent: $exponent")
    math.pow(2, exponent) / 0.77351
  }

  def union(other: ProbabilisticCounter): ProbabilisticCounter = {
    require(this.registers.length == other.registers.length)
    val newRegs = Array.ofDim[Int](registers.length)
    var r = 0
    while (r < registers.length) {
      newRegs(r) = this.registers(r) | other.registers(r)
      r += 1
    }
    new ProbabilisticCounter(newRegs)
  }

  override def toString: String =
    registers.map{ x =>
      val s = Integer.toBinaryString(x)
      String.format("%" + (32-s.length) + "s", s).replace(' ', '0')
    }.mkString("<",  " | ", ">")

  def canEqual(other: Any): Boolean = other.isInstanceOf[ProbabilisticCounter]

  override def equals(other: Any): Boolean = other match {
    case that: ProbabilisticCounter =>
      (that canEqual this) &&
        registers.deep == that.registers.deep
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(registers)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object ProbabilisticCounter {

  def lowestZeroBit(x: Int): Int = {
    var shifting = x
    var i = 0
    while ((shifting & 1) != 0) {
      i += 1
      shifting = shifting >>> 1
    }
    i
  }

  /**
   * Generates a random probabilistic counter, by setting for each register
   * the i-th bit to 1, with probability
   *
   * ```
   *            1
   *    p = ---------
   *         2^(i+1)
   * ```
   *
   * @param numRegisters
   * @return
   */
  def apply(numRegisters: Int): ProbabilisticCounter = {
    val registers = Array.ofDim[Int](numRegisters)

    var r = 0
    while (r < numRegisters) {

      // pick the index to set at 1 at random
      val rnd = Random.nextDouble()
      var threshold = 0.0
      var idx = 0
      while (idx < 32 && rnd >= threshold) {
        threshold += 1.0 / (1 << (idx+1))
        idx += 1
      }

      registers(r) = 1 << (idx - 1)

      r += 1
    }

    new ProbabilisticCounter(registers)
  }

  /**
   * Function to compute the hash function from node IDs.
   *
   * Taken from the WebGraph framework, specifically the class
   * IntHyperLogLogCounterArray.
   *
   * Note that the `x` parameter is a `Long`, but the function will also work
   * with `Int` values.
   *
   * @param x the element to hash, i.e. the node ID
   * @param seed the seed to set up internal state.
   * @return the hashed value of `x`
   */
  def jenkins( x: Long, seed: Long ): Long = {
    /* Set up the internal state */
    var a = seed + x
    var b = seed
    var c = 0x9e3779b97f4a7c13L /* the golden ratio; an arbitrary value */

    a -= b; a -= c; a ^= (c >>> 43)
    b -= c; b -= a; b ^= (a << 9)
    c -= a; c -= b; c ^= (b >>> 8)
    a -= b; a -= c; a ^= (c >>> 38)
    b -= c; b -= a; b ^= (a << 23)
    c -= a; c -= b; c ^= (b >>> 5)
    a -= b; a -= c; a ^= (c >>> 35)
    b -= c; b -= a; b ^= (a << 49)
    c -= a; c -= b; c ^= (b >>> 11)
    a -= b; a -= c; a ^= (c >>> 12)
    b -= c; b -= a; b ^= (a << 18)
    c -= a; c -= b; c ^= (b >>> 22)

    c
  }


}
