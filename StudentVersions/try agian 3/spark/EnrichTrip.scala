package bigdata.spark
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EnrichTrip extends App {

 val spark = SparkSession.builder()
   .appName("Spark Enrich Trip").master("local[*]")
   .getOrCreate()
 val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

 val enrichedStationSchema: StructType = StructType(List(
  StructField("system_id", StringType),
  StructField("timezone", StringType),
  StructField("station_id", IntegerType),
  StructField("name", StringType),
  StructField("short_name", StringType),
  StructField("lat", DoubleType),
  StructField("lon", DoubleType),
  StructField("capacity", IntegerType)
 ))
 val tripsSchema: StructType = StructType(List(
  StructField("route_id", StringType, nullable = true),
  StructField("service_id", StringType, nullable = true),
  StructField("trip_id", StringType, nullable = false),
  StructField("trip_headsign", StringType, nullable = true),
  StructField("direction_id", IntegerType, nullable = true),
  StructField("shape_id", IntegerType, nullable = true),
  StructField("wheelchair_accessible", IntegerType, nullable = false),
  StructField("note_fr", StringType, nullable = true),
  StructField("note_en", StringType, nullable = true)
 ))
 val filePath = "/user/bdss2001/rajesh/project5"

 val tripsDf: DataFrame = spark.read.option("header", "true").schema(tripsSchema).csv(s"$filePath/trips")
 tripsDf.toDF().show(10)

 val enrichedStationsDf: DataFrame = spark.read.format("parquet").load("/user/hive/warehouse/bdss2001_iris.db/enriched_station_information")
// val enrichedStationsDf: DataFrame = spark.read.csv("/user/hive/warehouse/bdss2001_rajesh.db/enriched_movie")
// enrichedStationsDf.toDF()
//   .show(10)
 val kafkaConfig: Map[String, String] = Map(
  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
  ConsumerConfig.GROUP_ID_CONFIG -> "spark-streaming-with-kafka",
  ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
  ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
 )

 val producerProperties = new Properties()
 producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.129.58:9092")
 producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
 producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
 producerProperties.setProperty("schema.registry.url", "http://172.16.129.58:8081")
 val producer = new KafkaProducer[String, GenericRecord](producerProperties)

 val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
  ssc,
  LocationStrategies.PreferConsistent,
  ConsumerStrategies.Subscribe[String, String](List("bdss2001_rajesh_trips"), kafkaConfig)
 )

 val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())

 kafkaStreamValues.foreachRDD(tripsRdd => {
  import spark.implicits._
  val tripsDF = tripsRdd.map(Trips(_)).toDF
  tripsDF.join(enrichedStationsDf, enrichedStationsDf.col("short_name") === tripsDF.col("start_station_code"))
  val enrichedTrips = tripsDF.collect().toList
  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  val metadata = srClient.getSchemaMetadata("bdss2001_rajesh_enriched_trip-value", 1)
  val EnrichedSchema = srClient.getByID(metadata.getId)
  val avroRecords: List[GenericRecord] = enrichedTrips.map { x =>
   val fields = x.toString().split(",")
   new GenericRecordBuilder(EnrichedSchema)
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
  avroRecords
    .map(avroMessage => new ProducerRecord[String, GenericRecord]("bdss2001_rajesh_enriched_trip-value",avroMessage.get("station_id").toString, avroMessage))
    .foreach(producer.send)

  producer.flush()
 }
 )
 ssc.start()
 ssc.awaitTermination()
 ssc.stop(stopSparkContext = true, stopGracefully = true)
 spark.stop()
}
