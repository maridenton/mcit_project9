package ca.mcit.bigdata.hadoop
import java.io.File
import com.github.agourlay.json2Csv.Json2Csv
//import ca.mcit.bigdata.hadoop.hadoopClient
import org.apache.hadoop.fs.Path
import scala.util.{Failure, Success}
import org.eclipse.jetty.server.Authentication.Failure

object Sprint  extends hadoopClient with hiveClient with App{

  val externalPath = new Path ( "/user/marinda/akhileshadala/external")

  val stationFromLocalFile = new File("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/station_information.json")
  val systemFromLocalFile = new File("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/system_information")

 // if (fs.exists(externalPath))
  //  fs.delete(externalPath, true)
println(fs.getUri)
  fs.mkdirs(new Path(s"$externalPath"))
println("created")
  val stationFile = fs.create(new Path(s"$externalPath/station_information/station_information.csv"))
  val systemFile= fs.create(new Path(s"$externalPath/system_information/system_information.csv"))


  Json2Csv.convert(stationFromLocalFile, systemFile)
  match{
    case exception: Exception=> println(s"exception: $exception")
    case Success(value) => println(s"$value lines written")
  }

 // Json2Csv.convert(stationFromLocalFile, systemFile)
//  match{
//    case Failure(exception: Exception)=> println(s"exception: $exception")
//    case Success(value) => println(s"$value lines written")
//  }

//  stmt.executeUpdate("Drop table if exists bdss2001_akhileshadala.ext_station_information")
//  stmt.executeUpdate("Drop table if exists bdss2001_akhileshadala.ext_system_information")
//
//  stmt.execute(
//    """CREATE EXTERNAL TABLE if not exists ext_station_information(
//      | system_id INT,
//      | language STRING,
//      | name STRING,
//      | short STRING,
//      | operator STRING,
//      | url STRING,
//      | purchase_url STRING,
//      | start_date STRING,
//      | phone_number STRING,
//      | email STRING,
//      | license_url STRING,
//      | timezone TIMESTAMP
//      | )
//      | ROW FORMAT DELIMITED
//      | FIELDS TERMINATED BY ','
//      | STORED AS TEXTFILE
//      | LOCATION '/user/bdss2001/akhileshadala/external/station_information''
//      | TBLPROPERTIES(
//      |  "skip.header.line.count" = "1",
//      |  "serialization.null.format" = ""
//      | )""".stripMargin
//  )
//  stmt.execute(
//    """CREATE EXTERNAL TABLE if not exists ext_system_information(
//      | system_id INT,
//      | language STRING,
//      | name STRING,
//      | short STRING,
//      | operator STRING,
//      | url STRING,
//      | purchase_url STRING,
//      | start_date STRING,
//      | phone_number STRING,
//      | email STRING,
//      | license_url STRING,
//      | timezone TIMESTAMP
//      | )
//      | ROW FORMAT DELIMITED
//      | FIELDS TERMINATED BY ','
//      | STORED AS TEXTFILE
//      | LOCATION '/user/bdss2001/akhileshadala/external/system_information'
//      | TBLPROPERTIES(
//      |  "skip.header.line.count" = "1",
//      |  "serialization.null.format" = ""
//      | )""".stripMargin
//  )
//  stmt.execute( "SET hive.exec.dynamic.partition =true")
//  stmt.execute( "SET hive.exec.dynamic.partition.mode =nonstrict")
//
//  stmt.execute(""" INSERT INTO TABLE enriched_station_information_ PARTITION (wheelchair_accesible)(
//                 |   select
//                 |   t.trip_id,
//                 |   t.service_id,
//                 |   t.route_id,
//                 |   t.trip_headsign,
//                 |   c.date,
//                 |   c.exeption_type,
//                 |   r.route_long_name,
//                 |   r.route_color,
//                 |   --t.wheelchair_accessible
//                 |   CASE t.wheelchair_accessible
//                 |   WHEN 1 then 1
//                 |   ELSE 0 END AS wheelchair_accessible
//                 |   from bdss2001_akhileshadala.ext_trips t
//                 |   inner join bdss2001_akhileshadala.ext_routes r on (t.route_id = r.route_id )
//                 |   inner join bdss2001_akhileshadala.ext_calendar_dates c on (t.service_id = c.service_id)""".stripMargin)
//  stmt.close()
//  connection.close()
}
