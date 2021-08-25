package ca.mcit.bigdata.sprint3

case class EnrichedStationsInfo(system_id: String, timezone: String, station_id: Int, name: String,
                                short_name: String, lat: Double, lon: Double, capacity: Int)
object EnrichedStationsInfo {
  def apply(csv: String): EnrichedStationsInfo = {
    val f = csv.split(",", -1)
    EnrichedStationsInfo(f(0), f(1), f(2).toInt, f(3), f(4), f(5).toDouble, f(6).toDouble, f(7).toInt)
  }
}