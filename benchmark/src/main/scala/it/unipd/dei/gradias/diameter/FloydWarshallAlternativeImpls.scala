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

import it.unipd.dei.gradias.{Distance, Infinity}

object FloydWarshallAlternativeImpls {

  def baseline(data: Array[Array[Distance]]): Array[Array[Distance]] = {
    val dim = data.length
    var k =0
    while(k<dim) {
      var i = 0
      while(i<dim) {
        var j= 0
        while(j<dim) {
          if(data(i)(k) != Infinity && data(k)(j) != Infinity) {
            val sum = data(i)(k) + data(k)(j)
            if(data(i)(j) > sum) {
              data(i)(j) = sum
            }
          }
          j += 1
        }
        i += 1
      }
      k += 1
    }
    data
  }

  def half(data: Array[Array[Distance]]): Array[Array[Distance]] = {
    val dim = data.length
    var k =0
    while(k<dim) {
      var i = 0
      while(i<dim) {
        var j = i
        while(j<dim) {
//          val (row, col) = if (i < j) (i,j) else (j,i)
          if(data(i)(k) != Infinity && data(k)(j) != Infinity) {
            val sum = data(i)(k) + data(k)(j)
            if(data(i)(j) > sum) {
              data(i)(j) = sum
              data(j)(i) = sum
            }
          }
          j += 1
        }
        i += 1
      }
      k += 1
    }

//    var i = 0
//    while(i<dim) {
//      var j = 0
//      while(j<i) {
//        data(i)(j) = data(j)(i)
//        j += 1
//      }
//      i += 1
//    }

    data
  }

}
