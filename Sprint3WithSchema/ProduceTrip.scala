package Sprint3WithSchema
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

import scala.io.{BufferedSource, Source}
object ProduceTrip {
  def main(args: Array[String]): Unit = {
    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](producerProperties)


    val topicName = "bdsf1901_marinda_trips"

    val dataPath = "/Users/marinda/Documents/1MCIT/Data/bixi_data/"
    val dataSource: BufferedSource = Source.fromFile(dataPath + "10_trips.csv")
    dataSource
      .getLines()
      .foreach(line => {
        producer.send(new ProducerRecord[String, String](topicName, line))
      })
    dataSource.close

    producer.flush()
    producer.close()
  }
}
