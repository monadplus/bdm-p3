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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.ml.PipelineModel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SparkSession

case class KafkaSample(neigh: String, price: Double)

object KafkaSample {
  def apply(record: ConsumerRecord[String, String]): KafkaSample = {
    val values = record.value().split(",")
    val nFields = 3
    if (values.length != nFields) {
      throw new RuntimeException(s"record has more than $nFields fields")
    }
    KafkaSample(values(1), values(2).toDouble)
  }
}

object Kafka extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[4]")
      .appName("myApp")
      .getOrCreate()

  import spark.implicits._

  // 2 seconds since the streaming produces an output every 1 second.
  val sc = new StreamingContext(spark.sparkContext, Seconds(2))

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

    val model = PipelineModel.load("datasets/lr-model")

    val stream: DStream[KafkaSample] =
      KafkaUtils
        .createDirectStream[String, String](
          sc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
        .map(record => KafkaSample(record))

    stream.foreachRDD { rdd =>
      // The columns must have the same name as the ones from the pipeline
      val df = rdd.toDF()
      val df_pred =
        model
          .transform(df)
          .select("neigh", "price", "prediction")
          .withColumnRenamed("neigh", "neighborhood")
          .withColumnRenamed("prediction", "rfd_pred")

      df_pred
        .repartition(1)
        .write
        .option("header", true)
        .mode("append")
        .csv("datasets/prediction")
      df_pred.show()
    }

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
