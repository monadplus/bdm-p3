package bdm

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SparkSession

object Algo extends App {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[4]")
      .appName("myApp")
      .getOrCreate()

  // 2 seconds since the streaming produces an output every 1 second.
  val sc = new StreamingContext(spark.sparkContext, Seconds(2))
  // StreamingContext.getOrCreate("datasets/checkpoints",
  //                              _ => new StreamingContext(spark.sparkContext, Seconds(2)))

  Logger.getRootLogger().setLevel(Level.ERROR)

  val topics = Array("bdm_p3")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "sandshrew.fib.upc.edu:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  try {

    val stream: DStream[KafkaSample] =
      KafkaUtils
        .createDirectStream[String, String](
          sc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
        .map(record => KafkaSample(record))

    // Save current state (required by most state operations)
    sc.checkpoint("datasets/checkpoints")

    if (args.length != 0) {
      args(0) match {
        case "1" => HeavyHitters.run(stream)
        case "2" => ExponentialDecayingWindow.run(stream)
        case _   => println("Usage:\n\t1 => HeavyHitters\n\t2 => ExponentialDecayingWindow")
      }
    } else {
      HeavyHitters.run(stream)
      // ExponentialDecayingWindow.run(stream)
    }

    sc.start()
    sc.awaitTermination()
  } catch {
    case e: Throwable =>
      print(e.getMessage())
  } finally {
    // TODO not stopping the stream after Ctr+c
    sc.stop(stopSparkContext = true)
  }
}
