import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Main extends App{
  //when you use spark session with builder contains spark content

  //when you use spark session with builder contains spark content
 val spark = SparkSession.builder().appName("sprint3").master(("local[*]")).getOrCreate()

val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

/*  val spark: SparkSession = SparkSession
    .builder()
    .appName("ExtractTripsStream")
    .master("local[2]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))*/


  /*This is my DataFrame that comes from HDFS in this case my file of word*/
  //val tripsDF = spark.read.option("header", "true").csv("/user/hive/warehouse/bdss2001_iris.db/enriched_station_information.parquet")
  //hdfs://quickstart.cloudera:8020/user/hive/warehouse/bdss2001_iris.db/enriched_station_information
  val enrichedStationInformationDF = spark
    .read
    .parquet("hdfs://quickstart.cloudera/user/hive/warehouse/bdss2001_iris.db/enriched_station_information")
  enrichedStationInformationDF.printSchema()
  enrichedStationInformationDF.show()
  // Parquet files can also be used to create a temporary view and then used in SQL statements

  //enrichedStationInformationDF.createOrReplaceTempView("parquetFile")
/*  val namesDF = spark.sql("SELECT system_id from enriched_station_information")
  namesDF.printSchema()*/


//
//  /*This code stream data*/
//  val  kafkaConfig: Map[String, String] = Map(
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "Localhost:9092",
//    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
//    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
//    ConsumerConfig.GROUP_ID_CONFIG -> "spark-streaming-with-kafka",
//    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
//    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
//  )
//
//  //Consume from kafka subject bdss2001_iguzman_trip
//  val kafkaStream: InputDStream[ConsumerRecord[String, String] ] = createDirectStream(ssc,
//    LocationStrategies.PreferConsistent,
//    ConsumerStrategies.Subscribe[String, String](List("bdss2001_iguzman_trip"), kafkaConfig)
//  )
//
//  //only if you want the value of the data
//  val kafkaStreamValues: DStream[String] = kafkaStream.map(_.value())
//
//  //what you you want to do
//  //each rdd have the input of Consumer of kafka(in this case bdss2001_iguzman_trip)
//  //kafkaStreamValues.foreachRDD(tripsRDD => processAndEnrich(tripsRDD))
//
//  /*----------Process, Transform, enrich and load the transformation------- */
///*  def processAndEnrich(tripsRDD: RDD[String]): Unit ={
//    val stopTimeDF =  tripsRDD.map(trip(_)).toDF
//    enrichedStationInformationDF.join(tripsDF, "route_id").write.mode(SaveMode.Append).csv("/user/bdss2001/iguzman/project4/data/data/")
//  }*/
//
//
//  ssc.start()
//  ssc.awaitTermination()
//  ssc.stop(stopSparkContext = true, stopGracefully = true)

}
