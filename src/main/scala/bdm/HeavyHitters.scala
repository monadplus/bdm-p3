package bdm

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

object HeavyHitters {
  val windowSize = Seconds(20)
  val minFreq = 0.5
  val maxSize = 1 / minFreq

  type CC = List[(String, Int)]

  def heavyHitters(a: CC, b: CC): CC = {
    val ab = a.union(b)
    if (ab.length > maxSize) {
      ab.map { case (e, count) => (e, count - 1) }
        .filter { case (_, count) => count > 0 }
    } else {
      ab
    }
  }

  def run(stream: DStream[KafkaSample]): Unit = {
    stream
      .map { case KafkaSample(neigh, _) =>
        List((neigh, 1))
      }
      .reduceByWindow(heavyHitters, windowSize, windowSize)
      .map(_.map(_._1))
      .print()
  }
}
