package sprint2

import scala.io.Source
import org.apache.hadoop.fs.Path
import org.apache.hive.jdbc.HiveDriver
import java.sql.DriverManager
import io.circe._
import io.circe.parser._

object jdbcMainObject extends hClient with App {

  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=marinda;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()
//
//  val externalPath = new Path("/user/fall2019/marinda/external")
//
//  //Check if Directory is exist or not, if exist then Delete
//  if (fs.exists(externalPath))
//    fs.delete(externalPath, true)
//
//  //Create Directories
//  fs.mkdirs(new Path(s"$externalPath"))
//  fs.mkdirs(new Path(s"$externalPath"+"/station_information"))
//  fs.mkdirs(new Path(s"$externalPath"+"/system_information"))
//
//  def systemInformation(): Unit = {
//
//    val systemInfoJsonFile: String = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/system_information.json").mkString
//
//    val docSystem: Json = parse(systemInfoJsonFile).getOrElse(Json.Null)
//    val cursor: HCursor = docSystem.hcursor
//    val systemCursor = cursor.downField("data")
//
//    val csvBuilderSystemInformation = new StringBuilder("system_id,time_zone\n")
//    val systemId = systemCursor.downField("system_id").as[String].getOrElse(Json.Null)
//    val timeZone = systemCursor.downField("timezone").as[String].getOrElse(Json.Null)
//    csvBuilderSystemInformation.append(systemId + "," + timeZone + "\n")
//
//    val os = fs.create(new Path("/user/fall2019/marinda/externalvikas/system_information/system_information.csv"))
//    os.write(csvBuilderSystemInformation.toString().getBytes())
//    os.close()
//  }
//
//  def stationInformation(): Unit = {
//    val stationInfoJsonFile: String = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/station_information.json").mkString
//
//    val docStation: Json = parse(stationInfoJsonFile).getOrElse(Json.Null)
//    val cursor: HCursor = docStation.hcursor
//    val stationArray = cursor.downField("data").downField("stations")
//    val csvBuilderStationInformation = new StringBuilder("station_id, name, short_name, lat, lon, capacity\n")
//
//    for (station <- stationArray.values.get) {
//      val stationCursor = station.hcursor
//
//      val stationId = stationCursor.downField("station_id").as[Int].getOrElse(Json.Null)
//      val name = stationCursor.downField("name").as[String].getOrElse(Json.Null)
//      val shortName = stationCursor.downField("short_name").as[String].getOrElse(Json.Null)
//      val lat = stationCursor.downField("lat").as[Double].getOrElse(Json.Null)
//      val lon = stationCursor.downField("lon").as[Double].getOrElse(Json.Null)
//      val capacity = stationCursor.downField("capacity").as[Int].getOrElse(Json.Null)
//
//      csvBuilderStationInformation.append(stationId + "," + name + "," + shortName + "," + lat +
//        "," + lon + "," + capacity + "\n")
//    }
//
//    val os = fs.create(new Path("/user/fall2019/marinda/externalvikas/station_information/station_information.csv"))
//    os.write(csvBuilderStationInformation.toString().getBytes())
//    os.close()
//  }
//
//  systemInformation()
//  stationInformation()
////
////  //Drop the table enriched_trip
// stmt.execute("drop table IF EXISTS fall2019_marinda.ext_station_information88")
//  stmt.execute("drop table IF EXISTS fall2019_marinda.ext_system_information88")
  stmt.execute("drop table IF EXISTS fall2019_marinda.enriched_station_information88")
////
//  stmt.execute(
//    """create external table fall2019_marinda.ext_station_information88(
//      |    station_id int,
//      |    external_id string,
//      |    name string,
//      |    short_name int,
//      |    lat double,
//      |    lon double,
//      |    rental_methods0 string,
//      |    rental_methods1 string,
//      |    capacity int,
//      |    electric_bike_surcharge_waiver boolean,
//      |    is_charging boolean,
//      |    eightd_has_key_dispenser boolean,
//      |    has_kiosk boolean
//      |)
//      |row format delimited
//      |fields terminated by ','
//      |stored as textfile
//      |location '/user/fall2019/marinda/externalvikas/station_information'
//      |tblproperties(
//      |    "skip.header.line.count" = "1",
//      |    "serialization.null.format" = "")""".stripMargin)
//
//  stmt.execute(
//    """create external table fall2019_marinda.ext_system_information88(
//      |   system_id string,
//      |   timezone string
//      |)
//      |row format delimited
//      |fields terminated by ','
//      |stored as textfile
//      |location '/user/fall2019/marinda/externalvikas/system_information'
//      |tblproperties(
//      |    "skip.header.line.count" = "1",
//      |    "serialization.null.format" = "")""".stripMargin)
//
  stmt.execute(
    """create table fall2019_marinda.enriched_station_information88(
      |   system_id string,
      |   timezone string,
      |   station_id int,
      |   name string,
      |   short_name string,
      |   lat double,
      |   long double,
      |   capacity int
      |)
      |stored as parquet
      |tblproperties(
      |    "parquet.compression" = "gzip")""".stripMargin)

//  //Set Partition mode as nonstrict
//  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
//  stmt.execute("set hive.mapred.mode=nonstrict")
//
//  println("hello")
//
//
//  //val qryRes = stmt.executeQuery(
//
//  //Insert data into enriched_station_information
//  stmt.execute(
//    """insert into table fall2019_marinda.enriched_station_information88
//      SELECT
//      |    sy.system_id,
//      |    sy.timezone,
//      |    st.station_id,
//      |    st.name,
//      |    st.short_name,
//      |    st.lat,
//      |    st.lon,
//      |    st.capacity
//      |FROM fall2019_marinda.ext_station_information88 as st
//      |    cross join fall2019_marinda.ext_system_information88 as sy""".stripMargin)
//println(qryRes)
 // SET hive.auto.convert.join=false;
  stmt.execute(
    """
       INSERT INTO TABLE fall2019_marinda.enriched_station_information88
       |SELECT
       |system.system_id,
       |system.timezone,
       |station.station_id,
       |station.name,
       |station.short_name,
       |station.lat,
       |station.lon,
       |station.capacity
       |FROM
       |bdss2001_vish.ext_system_information AS system
       |INNER JOIN
       |bdss2001_vish.ext_station_information AS station
    """.stripMargin)

}
