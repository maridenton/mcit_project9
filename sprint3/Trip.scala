package sprint3

case class Trip(
                 start_date: String,
                 start_station_code: Integer,
                 end_date: String,
                 end_station_code: Integer,
                 duration_second: Integer,
                 is_member: Integer
               )

object Trip {
  def apply(csv: String) : Trip = {
    val fields = csv.split(",")
    Trip(fields(0), fields(1).toInt, fields(2), fields(3).toInt, fields(4).toInt, fields(5).toInt)
  }
}