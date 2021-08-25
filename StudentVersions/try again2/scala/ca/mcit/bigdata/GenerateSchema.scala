package ca.mcit.bigdata
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import scala.io.Source

object GenerateSchema extends App {
  val SchemaText = Source.fromString(
    """{
      |  "type": "record",
      |  "name": "Enrichment",
      |  "namespace": "bigdata.kafka.schema",
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
  println(EnrichTripSchema.toString)
  val srClient = new CachedSchemaRegistryClient("http://quickstart.cloudera:8081", 1)
 srClient.register("bdss2001_Akhilesh_enriched_trip-value", EnrichTripSchema)
}