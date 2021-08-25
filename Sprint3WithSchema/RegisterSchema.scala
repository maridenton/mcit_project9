package Sprint3WithSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema

import scala.io.Source

object RegisterSchema extends App {
  val enrichedTripText = Source.fromString(
    """{
      |  "type": "record",
      |  "name": "EnrichedTripTest",
      |  "namespace": "ca.dataedu.avro",
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
      |    { "name":  "short_name", "type": "string" },
      |    { "name":  "lat", "type": "double" },
      |    { "name":  "lon", "type": "double" },
      |    { "name":  "capacity", "type": "int" }
      |  ]
      |}""".stripMargin
  )
    .mkString
  val enrichedTripSchema: Schema = new Schema.Parser().parse(enrichedTripText)
  val srClient = new CachedSchemaRegistryClient("http://quickstart.cloudera:8081", 1)
  srClient.register("fall2019_marinda_enriched_trip_test-value", enrichedTripSchema)
}
