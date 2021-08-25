package JsonWithCirce

case class Stations(station_id: String,
                    external_id: String,
                    name: String,
                    short_name: String,
                    lat: Float,
                    lon: Float,
                    rental_methods: List[String],
                    capacity: Int,
                    electric_bike_surcharge_waiver:Boolean,
                    is_charging:Boolean,
                    eightd_has_key_dispenser: Boolean,
                    has_kiosk: Boolean
                   )


object Stations {
  def toCsv(in: Stations): String = {
    s"${in.station_id}," +
      s"${in.external_id}," +
      s"${in.name}," +
      s"${in.short_name}," +
      s"${in.lat}," +
      s"${in.lon}," +
      s"${in.rental_methods}," +
      s"${in.capacity}," +
      s"${in.electric_bike_surcharge_waiver}," +
      s"${in.is_charging}," +
      s"${in.eightd_has_key_dispenser}," +
      s"${in.has_kiosk}\n"
  }
}