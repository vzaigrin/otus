package ru.otus.sparkmlstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer: KafkaProducer[String, String] = createProducer()
  def send(topic: String, value: String): Unit     = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(props: Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}
