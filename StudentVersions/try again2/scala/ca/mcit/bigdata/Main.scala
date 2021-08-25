package ca.mcit.bigdata
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

object Main extends App {
  // Register schema
  //  val SchemaText = Source.fromString(
  //    """{
  //      |  "type": "record",
  //      |  "name": "EnrichedTrip",
  //      |  "namespace": "big-data-project",
  //      |  "fields": [
  //      |    { "name":  "start_date", "type": "string" },
  //      |    { "name":  "start_station_code", "type": "int" },
  //      |    { "name":  "end_date", "type": "string" },
  //      |    { "name":  "end_station_code", "type": "int" },
  //      |    { "name":  "duration_sec", "type": "int" },
  //      |    { "name":  "is_member", "type": "int" },
  //      |    { "name":  "system_id", "type": "string" },
  //      |    { "name":  "timezone", "type": "string" },
  //      |    { "name":  "station_id", "type": "int" },
  //      |    { "name":  "name", "type": "string" },
  //      |    { "name":  "short_name", "type": "string" },
  //      |    { "name":  "lat", "type": "double" },
  //      |    { "name":  "lon", "type": "double" },
  //      |    { "name":  "capacity", "type": "int" }
  //      |  ]
  //      |}""".stripMargin
  //  ).mkString
  //
  //  val EnrichTripSchema = new Schema.Parser().parse(SchemaText)
  //
  //  val schemaRegistryClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)
  //  schemaRegistryClient.register("bdss2001_bhumi_enriched_trips-value", EnrichTripSchema)
  //should be in diffrent object



  import org.apache.log4j.{Level, Logger}
  val rootLogger : Logger = Logger.getRootLogger
  rootLogger.setLevel((Level.ERROR))
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark-project").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.kafka").setLevel(Level.ERROR)
  println("------------Schema Setup Done--------------")

  //Setup Spark
  val spark = SparkSession.builder()
    .appName("sprint-3").master("local[*]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  println("------------Spark Setup Done--------------")

  // Fetching Enriched Station Information from HDFS
  val hdfsLocation = "hdfs://quickstart.cloudera:8020/user/hive/warehouse/bdss2001_bhumi.db/station_information"
  val enrichedStationDf = spark.read
    .parquet(hdfsLocation)
  enrichedStationDf.show(2)
  println("------------Parquet File Read--------------")

  val kafkaConfig: Map[String, String] = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "bhumi",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[IntegerDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val topicName = "bdss2001_bhumi_trips_new"
  val kafkaStream: DStream[ConsumerRecord[Int, String]] = KafkaUtils.createDirectStream[Int, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[Int, String](List(topicName), kafkaConfig)
  )
  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

  kafkaStreamValues.foreachRDD(rdd => processAndEnrich(rdd))

  val enrichedTripTopicName = "bdss2001_bhumi_enriched_trip"
  val producerProperties = new Properties()
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  producerProperties.setProperty("schema.registry.url", "http://172.16.129.58:8081")

  val producer = new KafkaProducer[String, GenericRecord](producerProperties)

  println("------------Enriched Trip Producer Setup Done--------------")

  // Function to enrich streaming
  def processAndEnrich(rdd: RDD[String]): Unit = {
    import spark.implicits._
    val trip: RDD[Trips] = rdd.map(csvTrip => Trips(csvTrip))
    val tripDf = trip.toDF()
    tripDf.show(2)
    if (tripDf.isEmpty) {
      println("No new message received")
    }
    else {
      val enrichedData = enrichedStationDf
        .join(tripDf, tripDf("start_station_code") === enrichedStationDf("short_name"))
        .collect

      println(enrichedData)
      val schemaRegistry = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)

      val metadata = schemaRegistry
        .getSchemaMetadata("bdss2001_bhumi_enriched_trips-value", 1)

      val enrichedTripSchema = schemaRegistry.getByID(metadata.getId)

      val avroEnrichedTripList: List[GenericRecord] = enrichedData
        .map { item =>
          new GenericRecordBuilder(enrichedTripSchema)
            .set("start_date", item(8))
            .set("start_station_code", item(9))
            .set("end_date", item(10))
            .set("end_station_code", item(11))
            .set("duration_sec", item(12))
            .set("is_member", item(13))
            .set("system_id", item(0))
            .set("timezone", item(1))
            .set("station_id", item(2))
            .set("name", item(3))
            .set("short_name", item(4))
            .set("lat", item(5))
            .set("lon", item(6))
            .set("capacity", item(7))
            .build()
        }.toList
      /*
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

       */
      avroEnrichedTripList
        .foreach(line => {
          println(line)

          //  producer.send(new ProducerRecord[String, GenericRecord](enrichedTripTopicName, line))
        })

      producer.flush()

    }

  }

  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)
  spark.close()
}
