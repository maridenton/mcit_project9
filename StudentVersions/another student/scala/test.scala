import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {

    val myPath = "hdfs://172.16.129.58/user/fall2019/marinda/sprint2/e_station_information.csv"
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ExtractTripsStream")
      .master("local[2]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    // ===============================================================================================================
    // ===============================================================================================================
    val df: DataFrame = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(myPath)
      .select("system_id",
        "timezone",
        "station_short_name",
        "station_station_id",
        "station_name",
        "station_lat",
        "station_lon",
        "station_capacity")
    df.printSchema()

  }
}

