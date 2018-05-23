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

import scala.util.Random

object Statistics {

  def average(samples: Seq[Double]): Double = samples.sum / samples.size

  /**
   *
   * @param weights map in the form element -> count
   * @return
   */
  def weightedAverage(weights: Map[Int, Int]): Double =
    weights.toArray.map({case (k, freq) => k * freq}).sum.toDouble / weights.values.sum

  /**
   * Creates the map to be used by weightedAverage
   */
  def frequencyMap(samples: Seq[Double]): Map[Int, Int] =
    samples
      .map(_.toInt)
      .foldLeft(Map[Int, Int]())({(m, v) => m.updated(v, m.getOrElse(v,0) + 1)})

  def hist(samples: Seq[Double], scale: Double): String =
    hist(frequencyMap(samples), scale)

  def hist(freqs: Map[Int, Int], scale: Double): String =
    freqs
      .toArray
      .sorted
      .map{case (i, cnt) => "%4d - %5d ".format(i, cnt) + "#"*((cnt*scale).toInt)}
      .mkString("\n")

  def hist(freqs: Map[Int, Int]): String = {
    val scale = 100.0 / freqs.values.max
    hist(freqs, scale)
  }

  def rescale(samples: Seq[Double], maxValue: Double): Seq[Double] = {
    val max = samples.max
    if (max > maxValue) {
      val scaling = maxValue / max
      samples.map(_ * scaling)
    } else {
      samples
    }
  }

  trait Distribution extends Serializable {
    def sample: Double
  }

  class Exponential(lambda: Double) extends Distribution {

    def pdf(x: Double): Double = lambda * math.exp(-lambda*x)

    def sampleInt: Int = {
      var i = 0
      while(Random.nextDouble() < pdf(i)) {
        i += 1
      }
      return i
    }

    def sample: Double = {
      - math.log(Random.nextDouble()) / lambda
    }

  }

  class Uniform(low: Double, high: Double) extends Distribution {
    require(high > low)

    val spread = high - low

    def sample: Double = {
      (Random.nextDouble()*spread)+low
    }

  }

  class Skewed(probHeavy: Double = 0.1, lowValue: Double = 1.0e-6) extends Distribution {
    override def sample: Double = {
      if (Random.nextDouble() < probHeavy) {
        1.0
      } else {
        lowValue
      }
    }
  }

}
