package bdm

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

object HeavyHitters {
  def run(stream: DStream[KafkaSample]): Unit = {
    stream
      .map { case KafkaSample(neigh, _) =>
        (neigh, 1)
      }
      .groupByKeyAndWindow(Seconds(20))
      .map {case (k, xs) => (k, xs.toList.length)}
      .foreachRDD { rdd =>
        val xs: Array[(String, Int)] = rdd.collect()
        val total = xs.map(_._2).sum
        val perc = total*0.05
        val heavyHitters = xs.filter {case (_, count) => count >= perc}.map(_._1)
        println(heavyHitters.toList)
      }
  }
}
