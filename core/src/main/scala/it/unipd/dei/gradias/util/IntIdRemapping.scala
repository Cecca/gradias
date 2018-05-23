package it.unipd.dei.gradias.util

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

/**
 * An updatable remapping of integer IDs
 */
class IntIdRemapping {

  private var cnt = 0
  private val idmap = new Int2IntOpenHashMap()

  def apply(id: Int): Int = idmap.get(id)

  def update(id: Int): Int =
    if(!idmap.containsKey(id)) {
      idmap.put(id, cnt)
      val toret = cnt
      cnt += 1
      toret
    } else {
      idmap.get(id)
    }

}
