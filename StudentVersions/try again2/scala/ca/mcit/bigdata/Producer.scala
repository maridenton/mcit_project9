package ca.mcit.bigdata
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import scala.io.BufferedSource

object Producer extends App {
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  val tripProducer = new KafkaProducer[Int, String](producerProperties)
  val source: BufferedSource = scala.io.Source.fromFile("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/100_trips.csv")
  for (trip <- source.getLines) {
    tripProducer.send(new ProducerRecord[Int, String]("bdss2001_trips",trip))
  }
  tripProducer.flush()
}
