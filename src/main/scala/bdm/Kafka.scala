package bdm

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord

case class Msg(neighbor: String, price: Int)
object Msg {
  def apply(record: ConsumerRecord[String, String]): Msg = {
    val values = record.value().split(",")
    val nFields = 3
    if (values.length != nFields) {
      throw new RuntimeException(s"record has more than $nFields fields")
    }
    Msg(values(1), values(2).toInt)
  }
}

object Kafka extends App {
  val conf = new SparkConf().setAppName("app").setMaster("local[4]")
  val sc = new StreamingContext(conf, Seconds(1))

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
    val stream = KafkaUtils.createDirectStream[String, String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream
      .map(record => record.value)
      .print()

    sc.start()
    sc.awaitTermination()

  } catch {
    case e: Throwable =>
      print(e.getMessage())
  } finally {
    // TODO not stopping the stream after Ctr+c
    sc.stop()
  }
}
