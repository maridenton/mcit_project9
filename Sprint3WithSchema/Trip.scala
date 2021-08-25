package Sprint3WithSchema

case class Trip(start_date :String,
                start_station_code : Int,
                end_date :String,
                end_station_code :Int,
                duration_sec: Int,
                is_member :Int)