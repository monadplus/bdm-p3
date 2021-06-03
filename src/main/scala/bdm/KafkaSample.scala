package bdm

import org.apache.kafka.clients.consumer.ConsumerRecord

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
