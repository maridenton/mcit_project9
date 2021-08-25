package sprint3

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import scala.io.Source

object RegisterSchema extends App {
  val SchemaText = Source.fromString(
    """{
      |  "type": "record",
      |  "name": "EnrichedTrip",
      |  "namespace": "ca.mcit.bigdata.schema",
      |  "fields": [
      |    { "name":  "start_date", "type": "string" },
      |    { "name":  "start_station_code", "type": "int" },
      |    { "name":  "end_date", "type": "string" },
      |    { "name":  "end_station_code", "type": "int" },
      |    { "name":  "duration_sec", "type": "int" },
      |    { "name":  "is_member", "type": "int" },
      |    { "name":  "system_id", "type": "string" },
      |    { "name":  "timezone", "type": "string" },
      |    { "name":  "station_id", "type": "int" },
      |    { "name":  "name", "type": "string" },
      |    { "name":  "short_name", "type": "int" },
      |    { "name":  "lat", "type": "double" },
      |    { "name":  "lon", "type": "double" },
      |    { "name":  "capacity", "type": "string" }
      |  ]
      |}""".stripMargin
  ).mkString
  val EnrichTripSchema = new Schema.Parser().parse(SchemaText)
  val srClient = new CachedSchemaRegistryClient("http://172.16.129.58:8081", 1)
  srClient.register("bdss2001_mrvikkku_enriched_trip-value", EnrichTripSchema)
}

