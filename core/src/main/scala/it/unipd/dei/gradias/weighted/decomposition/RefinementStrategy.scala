package it.unipd.dei.gradias.weighted.decomposition

object RefinementStrategy {

  def get(description: String): RefinementStrategy = description match {
    case "stop" => Stop()
    case "exhaustion" => Exhaustion()
    case "half" => FractionOfPrevious(2)
    case "quarter" => FractionOfPrevious(4)
    case num => ConstantSteps(num.toInt)
  }

}

/**
 * This class represents a refinement
 * strategy for buckets of delta stepping
 */
sealed abstract class RefinementStrategy {

  def init(numDoneSteps: Int): RefinementStrategy

  def shouldStop: Boolean

  def shouldContinue: Boolean = !shouldStop

  def next: RefinementStrategy

}

case class ConstantSteps(numSteps: Int) extends RefinementStrategy {

  def init(numDoneSteps: Int): RefinementStrategy = this

  override def shouldStop: Boolean = numSteps == 0

  override def next: RefinementStrategy = ConstantSteps(numSteps - 1)

}

case class Stop() extends RefinementStrategy {

  def init(numDoneSteps: Int): RefinementStrategy = this

  override def shouldStop: Boolean = true

  override def next: RefinementStrategy = Stop()

}

case class Exhaustion() extends RefinementStrategy {

  def init(numDoneSteps: Int): RefinementStrategy = this

  override def shouldStop: Boolean = false

  override def next: RefinementStrategy = Exhaustion()

}

case class FractionOfPrevious(denominator: Int) extends RefinementStrategy {

  def init(numDoneSteps: Int): RefinementStrategy = ConstantSteps(numDoneSteps / denominator)

  override def shouldStop: Boolean = throw new UnsupportedOperationException()

  override def next: RefinementStrategy = throw new UnsupportedClassVersionError()

}