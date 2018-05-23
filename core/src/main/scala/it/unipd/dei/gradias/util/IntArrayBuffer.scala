package it.unipd.dei.gradias.util

/**
 * This class is to be used in place of ArrayBuffer, because ArrayBuffer
 * use a Array[AnyRef] as a backing array, thus resulting in the use of
 * boxed integers, that kill performance.
 *
 * @param initialSize
 */
class IntArrayBuffer(val initialSize: Int) {

  private var buffer: Array[Int] = Array.ofDim(initialSize)
  private var insertPosition: Int = 0

  private def ensureCapacity(size: Int): Unit = {
    if (size > buffer.length) {
      var newsize = buffer.length * 2
      while (size > newsize)
        newsize = newsize * 2

      val newar: Array[Int] = new Array(newsize)
      scala.compat.Platform.arraycopy(buffer, 0, newar, 0, buffer.length)
      buffer = newar
    }
  }

  def append(x: Int): Unit = {
    ensureCapacity(insertPosition+1)
    buffer(insertPosition) = x
    insertPosition += 1
  }

  def toArray: Array[Int] = {
    val arr = Array.ofDim[Int](insertPosition)
    scala.compat.Platform.arraycopy(buffer, 0, arr, 0, arr.length)
    arr
  }

}

object IntArrayBuffer {

  def apply(initialSize: Int = 16): IntArrayBuffer = new IntArrayBuffer(initialSize)

}
