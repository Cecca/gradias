package it.unipd.dei.gradias.util

/**
 * From Donald Knuth's Art of Computer Programming, Vol 2, page 232, 3rd edition.
 */
class RunningStats {

  private var cnt = 0L
  private var oldMean = 0.0
  private var newMean = 0.0
  private var oldSigma = 0.0
  private var newSigma = 0.0
  private var _min = Double.PositiveInfinity
  private var _max = 0.0

  def push(num: Double): Unit = {
    cnt += 1

    _max = math.max(_max, num)
    _min = math.min(_min, num)

    if(cnt == 1) {
      oldMean = num.toDouble
      newMean = num.toDouble
      oldSigma = 0.0
    } else {
      newMean = oldMean + (num - oldMean) / cnt
      newSigma = oldSigma + (num - oldMean)*(num - newMean)
      oldMean = newMean
      oldSigma = newSigma
    }
  }

  def mean: Double = newMean

  def variance: Double =
    if (cnt > 1) newSigma / (cnt -1)
    else 0.0

  def stddev: Double = math.sqrt(variance)

  def max: Double = _max
  def min: Double = _min
  def count: Long = cnt

  def merge(that: RunningStats): RunningStats = {
    // see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    val combined = new RunningStats()
    combined.cnt = this.cnt + that.cnt
    val delta = this.mean - that.mean

    combined.oldMean = (this.cnt*this.mean + that.cnt*that.mean) / (this.cnt + that.cnt).toDouble
    combined.newMean = combined.oldMean

    combined.oldSigma = this.newSigma + that.newSigma + math.pow(delta,2)*(this.cnt*that.cnt/combined.cnt.toDouble)
    combined.newSigma = combined.oldSigma

    combined._min = math.min(this.min, that.min)
    combined._max = math.max(this.max, that.max)

    combined
  }

}
