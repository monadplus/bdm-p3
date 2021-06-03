package bdm

import org.apache.spark.streaming.dstream.DStream

object ExponentialDecayingWindow {

  // Decay constant
  val c = 0.00001

  def go(newValues: Seq[Double], state: Option[Double]): Option[Double] = {
    if (newValues.length == 0) {
      state
    } else {
      val avg = newValues.sum / newValues.length
      state match {
        case None =>
          Some(avg)
        case Some(oldState) =>
          Some(oldState * (1 - c) + avg)
      }
    }
  }

  def run(stream: DStream[KafkaSample]): Unit = {
    stream
      .map { case KafkaSample(neigh, price) =>
        (neigh, price)
      }
      .updateStateByKey(go)
      .print()
  }
}
