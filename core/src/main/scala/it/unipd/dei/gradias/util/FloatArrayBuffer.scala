package it.unipd.dei.gradias.util

/**
 * This class is to be used in place of ArrayBuffer, because ArrayBuffer
 * use a Array[AnyRef] as a backing array, thus resulting in the use of
 * boxed floats, that kill performance.
 *
 * @param initialSize
 */
class FloatArrayBuffer(val initialSize: Int) {

  private var buffer: Array[Float] = Array.ofDim(initialSize)
  private var insertPosition: Int = 0

  private def ensureCapacity(size: Int): Unit = {
    if (size > buffer.length) {
      var newsize = buffer.length * 2
      while (size > newsize)
        newsize = newsize * 2

      val newar: Array[Float] = new Array(newsize)
      scala.compat.Platform.arraycopy(buffer, 0, newar, 0, buffer.length)
      buffer = newar
    }
  }

  def append(x: Float): Unit = {
    ensureCapacity(insertPosition+1)
    buffer(insertPosition) = x
    insertPosition += 1
  }

  def toArray: Array[Float] = {
    val arr = Array.ofDim[Float](insertPosition)
    scala.compat.Platform.arraycopy(buffer, 0, arr, 0, arr.length)
    arr
  }

}

object FloatArrayBuffer {

  def apply(initialSize: Int = 16): FloatArrayBuffer = new FloatArrayBuffer(initialSize)

}
