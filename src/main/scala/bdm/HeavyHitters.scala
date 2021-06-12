package bdm

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._
import scala.collection.immutable.TreeMap

object HeavyHitters {
  import bdm.TreeMapOps._

  val windowSize = Seconds(20)
  val minFreq = 0.5
  val maxSize = 1 / minFreq

  type Counter = TreeMap[String, Int]

  def heavyHitters(c1: Counter, c2: Counter): Counter = {
    val c = c1.unionWith(c2)(_ + _)
    if (c.size > maxSize) {
      c.map { case (e, count) => (e, count - 1) }
        .filter { case (_, count) => count > 0 }
    } else {
      c
    }
  }

  def run(stream: DStream[KafkaSample]): Unit = {
    stream
      .map { case KafkaSample(neigh, _) =>
        TreeMap((neigh, 1))
      }
      .reduceByWindow(heavyHitters, windowSize, windowSize)
      .map(_.map(_._1))
      .print()
  }
}
