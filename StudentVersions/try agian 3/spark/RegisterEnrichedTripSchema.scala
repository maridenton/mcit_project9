package bigdata.spark

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema

import scala.io.Source

object RegisterEnrichedTripSchema extends App {
  val SchemaText = Source.fromString(
    """{
      |  "type": "record",
      |  "name": "Enrich",
      |  "namespace": "bigdata.spark.avro",
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

  val EnrichedTripSchema = new Schema.Parser().parse(SchemaText)

  val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  srClient.register("enriched_trip-value", EnrichedTripSchema)
}
