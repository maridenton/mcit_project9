package sprint3

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object MainObject extends App with Base with HadoopClient {

  val hdfsPath = "/user/bdss2001/mrvikkku"

  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty("schema.registry.url", "http://172.16.129.58:8081")
  val producer = new KafkaProducer[String, GenericRecord](producerProperties)


  val schemaRegistry = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)
  val metadata = schemaRegistry.getSchemaMetadata("bdss2001_mrvikkku_enriched_trip-value", 1)
  val enrichedTripSchema = schemaRegistry.getByID(metadata.getId)

  val spark = SparkSession.builder()
    .master("local[*]").appName("Spark streaming with Kafka for Enrichment")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val enrichedStationDf = spark.read
    .option("header", "true")
    .parquet("/user/hive/warehouse/bdss2001_mrvikkku.db/enriched_station_information/")

  enrichedStationDf.show(10)

  /* ~~~~~~~~~~ Spark Streaming ~~~~~~~~~~ */
  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "sprintid",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )

  val topicName = "topic1"
  val kafkaStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topicName), kafkaConfig))

  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  kafkaStreamValues.foreachRDD(stopTimesRdd => processAndEnrichment(stopTimesRdd))

  /* ~~~~~~~~~~ Enrichment of Station Information and Trip ~~~~~~~~~~ */
  import spark.implicits._
  def processAndEnrichment(tripRdd: RDD[String]): Unit = {
    val tripsDF: DataFrame = tripRdd.map(Trip(_)).toDF
    tripsDF.show(10)
    val enrichedTripArray: Array[Row] = tripsDF
      .join(enrichedStationDf, tripsDF("start_station_code") === enrichedStationDf("short_name")).collect

    val avroEnrichedTrip: List[GenericRecord] = enrichedTripArray.map { fields =>
      new GenericRecordBuilder(enrichedTripSchema)
        .set("start_date", fields(1))
        .set("start_station_code", fields(0))
        .set("end_date", fields(2))
        .set("end_station_code", fields(3))
        .set("duration_sec", fields(4))
        .set("is_member", fields(5))
        .set("system_id", fields(6))
        .set("timezone", fields(7))
        .set("station_id", fields(8))
        .set("name", fields(9))
        .set("short_name", fields(10))
        .set("lat", fields(11))
        .set("lon", fields(12))
        .set("capacity", fields(13))
        .build()
    }.toList
    avroEnrichedTrip
      .foreach(line => {
        producer.send(new ProducerRecord[String, GenericRecord]("bdss2001_mrvikkku_enrichedtrip", line))
      })


    producer.flush()
  }

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)
  spark.close()

}
