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

package it.unipd.dei.gradias.diameter.dijkstra

import org.scalatest.{FreeSpec, Matchers}

import scala.util.Random

class PriorityQueueSpec extends FreeSpec with Matchers {

  "The queue must respect priority" - {

    "Small queue" in {
      val pq = new PriorityQueue(4)

      pq.enqueue(0,1)
      pq.enqueue(1,2)
      pq.enqueue(2,3)
      pq.enqueue(3,0)

      pq.dequeue() should be (3)
      pq.dequeue() should be (0)
      pq.dequeue() should be (1)
      pq.dequeue() should be (2)
    }

    "Big queue" in {

      val elems = Vector(
        (86,4),
        (90,1),
        (1,3),
        (10,6),
        (14,2),
        (56,5),
        (60,0),
        (99,7)
      )

      val pq = new PriorityQueue(elems.size)

      for((pri, v) <- elems) {
        pq.enqueue(v, pri)
        pq.heap should contain (v)
        pq.weight should contain (pri)
      }
      pq.isHeap(1) should be (true)

      pq.dequeue() should be (3)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (6)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (2)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (5)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (0)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (4)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (1)
      pq.isHeap(1) should be (true)
      pq.dequeue() should be (7)
      pq.isHeap(1) should be (true)

    }
  }

  "Helper methods" in {

    val pq = new PriorityQueue(4)

    pq.empty should be (true)
    pq.nonEmpty should be (false)

    pq.enqueue(0, 1)

    pq.empty should be (false)
    pq.nonEmpty should be (true)

    pq.dequeue()

    pq.empty should be (true)
    pq.nonEmpty should be (false)
  }

  "Random queue" in {
    val dim = Random.nextInt(80)
    // vector of random pairs with no duplicate keys.
    // Duplicate keys are no problem for the priority queue (the dequeue order is not relevant),
    // but are a problem for the test. Hence we remove them.
    val vals = Range(0, dim).map(i => (Random.nextInt(100), i)).toMap.toVector
    val pq = new PriorityQueue(dim)

    for((pri, v) <- vals) {
      pq.enqueue(v, pri)
    }

    for((pri, v) <- vals.sortBy(_._1)) {
      pq.dequeue() should equal (v)
    }

  }

}
