package ca.mcit.bigdata
case class Trips(startDate:String, start_station_code : Int, end_date: String, end_station_code: Int,
                 duration_sec: Int, is_member: Int)

object Trips{
  def apply(csv : String) : Trips ={
    val fields = csv.split(",")
    Trips(fields(0),fields(1).toInt,fields(2),fields(3).toInt,fields(4).toInt,fields(5).toInt)
  }
}