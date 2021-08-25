package JsonWithCirce

import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._

import java.io.{BufferedWriter, File, FileWriter}


object main extends App {

  implicit val systemInfoDecoder: Decoder[SystemInfo] = deriveDecoder
  implicit val systemDataDecoder: Decoder[SystemData] = deriveDecoder

  implicit val stationInfoDecoder: Decoder[StationInfo] = deriveDecoder
  implicit val stationDataDecoder: Decoder[StationData] = deriveDecoder
  implicit val stationDecoder: Decoder[Stations] = deriveDecoder
  //  implicit val systemInfoEncoder: Encoder[SystemData] = deriveEncoder
  //  implicit val systemDataEncoder: Encoder[SystemInfo] = deriveEncoder
  //  implicit val stationInfoEncoder: Encoder[StationData] = deriveEncoder
  //  implicit val stationDataEncoder: Encoder[StationInfo] = deriveEncoder
  //  implicit val stationEncoder: Encoder[Stations] = deriveEncoder


  // ==  Decoding the System_Information file  ==
  val systemInfoSource = scala.io.Source.fromFile("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/system_information.json")
  val systemInfoLines = try systemInfoSource.mkString finally systemInfoSource.close()

  decode[SystemInfo](systemInfoLines) match {
    case Left(e) =>
      println(s"Ooops some errror here 1 ${e}")
    case Right(v) =>
      val outFile = new BufferedWriter(
        new FileWriter(
          new File("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/system_information.csv")))
      outFile.write(SystemData.toCsv(v.data))
      outFile.close()
  }


  // ==  Decoding the System_Information file  ==
  val stationInfoSource = scala.io.Source.fromFile("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/station_information.json")
  val stationInfoLines = try stationInfoSource.mkString finally stationInfoSource.close()

  decode[StationInfo](stationInfoLines) match {
    case Left(e) =>
      println(s"Ooops some errror here ${e}")
    case Right(v) =>
      val outFile = new BufferedWriter(
        new FileWriter(
          new File("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/station_information.csv")))
      v.data.stations
        .map(line => Stations.toCsv(line))
        .foreach(line => outFile.write(line))
      outFile.close()
  }
}