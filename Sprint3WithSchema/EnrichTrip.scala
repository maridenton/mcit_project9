package Sprint3WithSchema
import java.util.Properties
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}



import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Properties


object EnrichTrip {
  def main(args: Array[String]): Unit = {

    val eStationPath = "hdfs://172.16.129.58/user/fall2019/marinda/sprint2/e_station_information.csv"
    //val eTripPath = "hdfs://172.16.129.58/user/fall2019/marinda/sprint3/e_trip_information.csv"

    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExtractTripsStream")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    import spark.implicits._
    // ===============================================================================================================
    val producerProperties = new Properties()
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://quickstart.cloudera:9092")
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
    producerProperties.setProperty("schema.registry.url", "http://quickstart.cloudera:8081")

    val producer = new KafkaProducer[String, GenericRecord](producerProperties)
    val pTopic = "fall2019_marinda_enriched_trip_test"

    val srClient = new CachedSchemaRegistryClient("http://quickstart.cloudera:8081", 1)
    val metadata = srClient.getSchemaMetadata("fall2019_marinda_enriched_trip_test-value", 1)
    val eTripSchema: Schema = srClient.getByID(metadata.getId)

    // ===============================================================================================================
    val consumerConfig = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "quickstart.cloudera:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.GROUP_ID_CONFIG -> "Group_1",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
    val topic = "bdsf1901_marinda_trips"

    val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), consumerConfig)
    )
    // ===============================================================================================================
    


    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(eStationPath)
      .select("system_id",
        "timezone",
        "station_short_name",
        "station_station_id",
        "station_name",
        "station_lat",
        "station_lon",
        "station_capacity")
    //        .withColumnRenamed("station_short_name", "start_station_code")
    //        .withColumnRenamed("station_station_id", "station_id")
    //        .withColumnRenamed("station_name", "name")
    //        .withColumnRenamed("station_lat", "lat")
    //        .withColumnRenamed("station_lon", "lon")
    //        .withColumnRenamed("station_capacity", "capacity")


    val eStationDf: DataFrame = df.selectExpr("system_id",
      "timezone",
      "cast(station_short_name as int) start_station_code",
      "cast(station_station_id as int) station_id",
      "station_name name",
      "station_short_name short_name",
      "cast(station_lat as double) lat",
      "cast(station_lon as double) lon",
      "cast(station_capacity as int) capacity")

    df.printSchema()
    eStationDf.printSchema()
    eStationDf.show()
    //  df.show
    // ===============================================================================================================

    inStream.map(_.value()).foreachRDD(mBatch => processMb(mBatch))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5)
    ssc.stop(stopSparkContext = true, stopGracefully = true)

    producer.flush()
    producer.close()


    // ===============================================================================================================
    // * Process each MicroBatch (RDD)
    def processMb(rdd: RDD[String]): Unit = {

      val tripDf = rdd.map(_.split(",", -1))
        .map(n => Trip(n(0), n(1).toInt, n(2), n(3).toInt, n(4).toInt, n(5).toInt))
        .toDF()
      tripDf.printSchema()

      //          tripDf.join(eStationDf, "start_station_code").printSchema()
      val enrichTripList = tripDf.join(eStationDf, "start_station_code").collect()

      //  for (a <- enrichTripList)println(a(13))
      //  println(enrichTripList(1))
      //   for (a <- enrichTripList)println(s"$a(11),$a(12),$a(13)")


      val avroEnrichedTrip: List[GenericRecord] = enrichTripList.map { x =>
        new GenericRecordBuilder(eTripSchema)
          .set("start_date", x(1))
          .set("start_station_code", x(0))
          .set("end_date", x(2))
          .set("end_station_code", x(3))
          .set("duration_sec", x(4))
          .set("is_member", x(5))
          .set("system_id", x(6))
          .set("timezone", x(7))
          .set("station_id", x(8))
          .set("name", x(9))
          .set("short_name", x(10))
          .set("lat", x(11))
          .set("lon", x(12))
          .set("capacity", x(13))
          .build()
      }.toList
      //  avroEnrichedTrip.foreach(println)

      avroEnrichedTrip
        .foreach(line => {
          producer.send(new ProducerRecord[String, GenericRecord](pTopic, line))
        })

      //        avroEnrichedTrip
      //          .map(x => new ProducerRecord[String, GenericRecord](pTopic, x.get("start_date").toString, x))
      //          .foreach(producer.send)
    }
  }
}

//
//{ "name":  "start_date", "type": "string" },
//{ "name":  "start_station_code", "type": "int" },
//{ "name":  "end_date", "type": "string" },
//{ "name":  "end_station_code", "type": "int" },
//{ "name":  "duration_sec", "type": "int" },
//{ "name":  "is_member", "type": "int" },
//{ "name":  "system_id", "type": "string" },
//{ "name":  "timezone", "type": "string" },
//{ "name":  "station_id", "type": "int" },
//{ "name":  "name", "type": "string" },
//{ "name":  "short_name", "type": "string" },
//{ "name":  "lat", "type": "double" },
//{ "name":  "lon", "type": "double" },
//{ "name":  "capacity", "type": "int" }