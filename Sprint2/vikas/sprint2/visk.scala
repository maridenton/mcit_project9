package sprint2
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json, parser}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager
import scala.io.Source
object visk extends App with hClient {

  Class.forName(classOf[HiveDriver].getName)
  val connectionString = "jdbc:hive2://172.16.129.58:10000/;user=marinda;"
  val connection = DriverManager.getConnection(connectionString)
  val stmt = connection.createStatement()
 
    
    //Create database
  //  stmt.execute("CREATE DATABASE IF NOT EXISTS fall2019_marinda")
    //Manage tables
    stmt.execute("DROP TABLE fall2019_marinda.ext_system_information77")
    stmt.execute("DROP TABLE fall2019_marinda.ext_station_information77")
    stmt.execute("DROP TABLE fall2019_marinda.enriched_station_information77")
    //external table of system information
    stmt.execute(
      """create external table if not exists fall2019_marinda.ext_system_information77(
        |system_id STRING,
        |language STRING,
        |name STRING,
        |short_name STRING,
        |operator STRING,
        |url STRING,
        |purchase_url STRING,
        |start_date STRING,
        |phone_number STRING,
        |email STRING,
        |timezone STRING,
        |license_url STRING
        |) ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ','
        |STORED AS TEXTFILE
        |LOCATION '/user/bdss2001/vish1/external/system_information/'
        """.stripMargin)
    //external table of station information
    stmt.execute(
      """create external table if not exists fall2019_marinda.ext_station_information77(
        |station_id INT,
        |external_id STRING,
        |name STRING,
        |short_name STRING,
        |lat DOUBLE,
        |lon DOUBLE,
        |rental_methods STRING,
        |capacity INT,
        |electric_bike_surcharge_waiver BOOLEAN,
        |is_charging BOOLEAN,
        |eightd_has_key_dispenser BOOLEAN,
        |has_kiosk BOOLEAN
        |) ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ','
        |STORED AS TEXTFILE
        |LOCATION '/user/bdss2001/vish1/external/station_information/'
           """.stripMargin)
    //create table for enriched station information
    stmt.execute(
      """create table if not exists fall2019_marinda.enriched_station_information77(
        |system_id STRING,
        |timezone STRING,
        |station_id INT,
        |name STRING,
        |short_name STRING,
        |lat DOUBLE,
        |lon DOUBLE,
        |capacity INT
        |)
        |ROW FORMAT DELIMITED
        |FIELDS TERMINATED BY ','
        |STORED AS PARQUET
           """.stripMargin)
    //insert data using cross join
    stmt.execute(
      """INSERT INTO TABLE fall2019_marinda.enriched_station_information77
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
         |fall2019_marinda.ext_system_information77 AS system
         |INNER JOIN
         |fall2019_marinda.ext_station_information77 AS station
    """.stripMargin)
    stmt.close()
    connection.close()

}
