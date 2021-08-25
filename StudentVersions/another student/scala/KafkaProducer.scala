import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import java.util.Properties

object KafkaProducer extends App{

  val topicName = "bdss2001_iguzman_trip"

  val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName
  )
  //get the value of the serialization
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )
  //instantiate a producer with a key of
  val producer = new KafkaProducer[Int, String](producerProperties)

  // --Read file and add it to the topic
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 07:54:26.237,262,2021-04-09 08:09:55.273,410,929,1"))
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 07:59:40.044,171,2021-04-09 08:04:38.485,148,298,1"))
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 08:02:23.059,636,2021-04-09 08:12:13.563,392,590,1"))
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 08:03:03.069,636,2021-04-09 08:30:24.336,785,1641,1"))
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 08:06:21.594,907,2021-04-09 08:18:38.855,152,737,1"))
  producer.send(new ProducerRecord[Int, String](topicName, 10, "2021-04-09 08:06:58.553,566,2021-04-09 08:52:00.672,365,2702,1"))

  producer.flush()

}
