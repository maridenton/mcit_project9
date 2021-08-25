package ca.mcit.bigdata
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TripEnrichment extends App with Base {

  val spark = SparkSession.builder().appName("Enrich Trips").master("local[*]").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
 // val tripsDataFrame: DataFrame = spark.read.option("header", "true").option("inferschema", "true").csv("/user/bdss2001/akhileshadala/project5/trips/trips.txt")
  val stationInfoDF: DataFrame = spark.read.format("csv").load("/user/hive/warehouse/bdss2001_akhileshadala.db/enriched_station_information")
  stationInfoDF.toDF()
  val kafkaConsumerConfig: Map[String, String] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "trips",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List("bdss2001_akhileshadala_trips"), kafkaConsumerConfig)
  )

  val tripValues: DStream[String] = kafkaStream.map(_.value())
  tripValues.foreachRDD(tripsRDD => {
    import spark.implicits._
    val tripsDF = tripsRDD.map(Trips(_)).toDF
    val output = tripsDF.join(stationInfoDF, tripsDF("start_station_code") === stationInfoDF("short_name"))
    val tripsList = output.collect().toList
    val schemaRegistry = new CachedSchemaRegistryClient("http://localhost:8081", 1)
    val metadata = schemaRegistry.getSchemaMetadata("bdss2001_akhileshadala_enriched_trip-value", 1)
    val EnrichedTripSchema = schemaRegistry.getByID(metadata.getId)
    val enrichedRecords: List[GenericRecord] = tripsList.map { x => val fields = x.toString().split(",")
      new GenericRecordBuilder(EnrichedTripSchema)
        .set("start_date", fields(0))
        .set("start_station_code", fields(1).toInt)
        .set("end_date", fields(2))
        .set("end_station_code", fields(3).toInt)
        .set("duration_sec", fields(4).toInt)
        .set("is_member", fields(5).toInt)
        .set("system_id", fields(6))
        .set("timezone", fields(7))
        .set("station_id", fields(8).toInt)
        .set("name", fields(9))
        .set("short_name", fields(10).toInt)
        .set("lat", fields(11).toDouble)
        .set("lon", fields(12).toDouble)
        .set("capacity", fields(13))
        .build()
    }
    println(enrichedRecords)
    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://localhost:8081")
    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    enrichedRecords.map(records => new ProducerRecord[String, GenericRecord]
    ("bdss2001_akhileshadala_enriched_trip-value", records.get("station_id").toString, records))
      .foreach(producer.send)
    producer.flush()
  }
  )
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(stopSparkContext = true, stopGracefully = true)
  spark.stop()
}
