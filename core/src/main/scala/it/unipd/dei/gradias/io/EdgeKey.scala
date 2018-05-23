package it.unipd.dei.gradias.io

import java.io.{DataOutput, DataInput}

import org.apache.hadoop.io.{WritableComparable, Writable}

class EdgeKey(var src: Long, var dst: Long) extends WritableComparable[EdgeKey] with Serializable {

  def this() {
    this(-1, -1)
  }

  override def write(out: DataOutput): Unit = {
    out.writeLong(src)
    out.writeLong(dst)
  }

  override def readFields(in: DataInput): Unit = {
    src = in.readLong()
    dst = in.readLong()
  }


  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: EdgeKey =>
        this.src == that.src && this.dst == that.dst
      case _ => false
    }
  }

  override def toString: String = s"$src -> $dst"

  override def hashCode(): Int = (src % Int.MaxValue).toInt

  override def compareTo(that: EdgeKey): Int = {
    if(this.src < that.src)      -1
    else if(this.src > that.src)  1
    else if(this.dst < that.dst) -1
    else if(this.dst > that.dst)  1
    else                          0
  }
}
