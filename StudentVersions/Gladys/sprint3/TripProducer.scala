package ca.mcit.bigdata.sprint3

import java.util.Properties
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object TripProducer extends App with Config {

    val topicName = "bdss2001_vish_trip"

    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[Int, String](producerProperties)

    val inputStream: FSDataInputStream = fs
      .open(new Path("D:\\Project Bigdata\\Code\\Sprint3\\Data\\100_trips.csv"))

    var i = 1
    val data: Unit = scala.io.Source.fromInputStream(inputStream)
      .getLines()
      .drop(1)
      .take(10)
      .mkString("\n")
      .split("\n")
      .foreach(line => {
          println(line)
          producer.send(new ProducerRecord[Int, String](topicName, i, line))
          i = i + 1
      })
    producer.flush()
}
