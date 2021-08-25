package JsonWithCirce

case class SystemData(system_id: String,
                      language: String,
                      name: String,
                      short_name: String,
                      operator: String,
                      url: String,
                      purchase_url: String,
                      start_date: String,
                      phone_number: String,
                      email: String,
                      license_url: String,
                      timezone: String
                     )
object SystemData {
  def toCsv(in: SystemData): String = {
    s"${in.system_id}," +
      s"${in.language}," +
      s"${in.name}," +
      s"${in.short_name}," +
      s"${in.operator}," +
      s"${in.url}," +
      s"${in.purchase_url}," +
      s"${in.start_date}," +
      s"${in.phone_number}," +
      s"${in.email}," +
      s"${in.license_url}," +
      s"${in.timezone}\n"
  }
}