package ca.mcit.bigdata.hadoop
import java.io.File
import com.github.agourlay.json2Csv.Json2Csv
import org.apache.hadoop.fs.Path

object Sprint  extends hadoopClient with hiveClient with App{

//  val externalPath = new Path ("/user/bdss2001/akhileshadala/external")
//
//  val stationFromLocalFile
//  = new File("/Users/akhileshreddyadala/Desktop/Scala/project/sprint2/station_information.json")
//  val systemFromLocalFile
//  = new File("/Users/akhileshreddyadala/Desktop/Scala/project/sprint2/system_information.json")
//
//  if (fs.exists(externalPath))
//    fs.delete(externalPath, true)
//
//  fs.mkdirs(new Path(s"$externalPath"))
//
//  val stationFile = fs.create(new Path(s"$externalPath/station_information/station_information.csv"))
//  val systemFile= fs.create(new Path(s"$externalPath/system_information/system_information.csv"))
//
//  Json2Csv.convert(stationFromLocalFile, stationFile)
//  Json2Csv.convert(systemFromLocalFile, systemFile)

//  stmt.executeUpdate("Drop table if exists bdss2001_akhileshadala.ext_station_information")
//  stmt.executeUpdate("Drop table if exists bdss2001_akhileshadala.ext_system_information")
//
//  stmt.execute(
//    """CREATE EXTERNAL TABLE if not exists bdss2001_akhileshadala.ext_station_information(
//      | capacity INT,
//      | eightHasKeyDispenser BOOLEAN,
//      | electricBikeSurchargeWaiver BOOLEAN,
//      | externalId INT,
//      | hasKiosk BOOLEAN,
//      | isCharging BOOLEAN,
//      | lat DOUBLE,
//      | lon DOUBLE,
//      | name STRING,
//      | rentalMethodsCc STRING,
//      | rentalMethodsKey STRING,
//      | shortName STRING,
//      | stationId STRING,
//      | lastUpdated STRING,
//      | ttl INT
//      | )
//      | ROW FORMAT DELIMITED
//      | FIELDS TERMINATED BY ','
//      | STORED AS TEXTFILE
//      | LOCATION '/user/bdss2001/akhileshadala/external/station_information'
//      | TBLPROPERTIES(
//      |  "skip.header.line.count" = "1",
//      |  "serialization.null.format" = ""
//      | )""".stripMargin
//  )
//  stmt.execute(
//    """CREATE EXTERNAL TABLE if not exists bdss2001_akhileshadala.ext_system_information(
//      | email STRING,
//      | language STRING,
//      | licenseUrl STRING,
//      | name STRING,
//      | operator STRING,
//      | phoneNumber STRING,
//      | purchaseUrl STRING,
//      | shortName STRING,
//      | startDate STRING,
//      | systemId STRING,
//      | timezone STRING,
//      | url STRING,
//      | lastUpdated STRING,
//      | ttl INT
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
  stmt.execute(""" INSERT INTO TABLE bdss2001_akhileshadala.enriched_station_information
                 |   select
                 |   t.systemId,
                 |   t.timezone,
                 |   s.stationId,
                 |   s.name,
                 |   s.shortName,
                 |   s.lat,
                 |   s.lon,
                 |   s.capacity
                 |   from bdss2001_akhileshadala.ext_system_information AS t
                 |   cross join bdss2001_akhileshadala.ext_station_information AS s
                 |   """.stripMargin)
  stmt.close()
  connection.close()
}