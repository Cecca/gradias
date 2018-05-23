package it.unipd.dei.gradias.io

import java.io.{DataOutput, DataInput}

import org.apache.hadoop.io.Writable

/**
 * An edge whose weight is a `Long`. Used for serialization.
 */
class EdgeLongWeight(var src: Long, var dst: Long, var weight: Long) extends Writable {

  /**
   * Auxiliary constructor for deserialization of sequence files
   */
  def this() {
    this(-1, -1, -1)
  }

  override def write(out: DataOutput): Unit = {
    out.writeLong(src)
    out.writeLong(dst)
    out.writeLong(weight)
  }

  override def readFields(in: DataInput): Unit = {
    src = in.readLong()
    dst = in.readLong()
    weight = in.readLong()
  }

}
