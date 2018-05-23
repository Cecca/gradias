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

import it.unipd.dei.gradias.{Distance, Infinity, NodeId}

import scala.annotation.tailrec

/**
 * Implementation of a fixed max-size priority queue to be used in
 * Dijkstra algorithm. Internally it uses a min-heap.
 *
 * Can store values in the range [0..maxSize-1].
 *
 * @param maxSize the maximum size
 */
class PriorityQueue(val maxSize: Int) {

  val heap = Array.fill[NodeId](maxSize+1)(-1)

  val weight = Array.fill[Distance](maxSize)(Infinity)

  val position = Array.fill(maxSize)(-1)

  private var _size = 0

  def empty: Boolean = _size < 1

  def nonEmpty: Boolean = !empty

  def parent(i: Int): Int = i / 2

  def right(i: Int): Int = 2*i + 1

  def left(i: Int): Int = 2*i

  def isHeap(i: Int): Boolean = {
    if(i > _size)
      return true
    if(
      (left(i) <= _size &&  weight(heap(i)) > weight(heap(left(i)))) ||
        (right(i) <= _size && weight(heap(i)) > weight(heap(right(i)))))
      return false
    else
      return isHeap(left(i)) && isHeap(right(i))
  }

  @tailrec
  final def heapify(i: Int): Unit = {
    val l = left(i)
    val r = right(i)

    var smallest =
      if (l <= _size && weight(heap(l)) < weight(heap(i)))
        l
      else i
    smallest =
      if (r <= _size && weight(heap(r)) < weight(heap(smallest)))
        r
      else
        smallest

    if (smallest != i){
      val rootVal = heap(i)
      val childVal = heap(smallest)
      heap(i) = childVal
      heap(smallest) = rootVal
      position(childVal) = i
      position(rootVal) = smallest
      heapify(smallest)
    }
  }

  def enqueue(x: NodeId, w: Distance): Unit = {
    if(x >= maxSize)
      throw new IllegalArgumentException(
        "Can store only elements less than the maximum heap size")

    _size += 1
    heap(_size) = x
    position(x) = _size
    weight(x) = Infinity
    decreasePriority(x, w)
  }

  def dequeue(): Int = {
    if(_size < 1)
      throw new IndexOutOfBoundsException("Cannot dequeue an empty queue")

    val min = heap(1)
    heap(1) = heap(_size)
    position(heap(1)) = 1
    heap(_size) = -1 // TODO remove this, not necessary
    _size -= 1
    position(min) = -1
    heapify(1)
    min
  }

  def decreasePriority(x: NodeId, w: Distance) = {
    if(w > weight(x))
      throw new IllegalArgumentException("The new weight is greater than the previous weight")
    if(position(x) < 0)
      throw new IllegalArgumentException(s"Cannot decrease priority of an element not in the queue: $x")

    weight(x) = w
    var i = position(x)
    var par = parent(i)
    while(i > 1 && weight(heap(par)) > w) {
      val parVal = heap(par)
      heap(i) = parVal
      heap(par) = x
      position(parVal) = i
      position(x) = par
      i = parent(i)
      par = parent(i)
    }
  }

  override def toString: String =
    s"""
      | Heap      = ${heap.tail.toList.map(x => f"$x%4d")}
      | Weights   = ${heap.tail.toList.map(x => if(x>=0) f"${weight(x)}%4.4f" else "  -1")}
      | Positions = ${position.toList.map(x => f"$x%4d")}
    """.stripMargin

}
