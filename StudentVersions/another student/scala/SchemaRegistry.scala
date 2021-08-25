import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema

import scala.io.Source

object SchemaRegistry extends App{

  val enrichedTripSchemaText = Source.fromString(
    """{
      |  "type": "record",
      |  "name": "Movie",
      |  "namespace": "enriched_trip",
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
  // 2. Parse schema
  val enrichedTripSchema: Schema = new Schema.Parser().parse(enrichedTripSchemaText)
  //val enrichedTripSchema = new Schema.Parser().parse(enrichedTripSchemaText)

  // 3. Create a client
  //val srClient = new CachedSchemaRegistryClient("http://localhost:8081", 1)
  val srClient = new CachedSchemaRegistryClient("http://quickstart.cloudera:8081", 1)
  srClient.register("BDSS01_Iguzman_enriched_trip_value", enrichedTripSchema)

  // 3. register the schema
/*  val id = srClient.register("BDSS01_Iguzman_enriched_trip_value", new AvroSchema(enrichedTripSchema).asInstanceOf[ParsedSchema])
  srClient.register()*/

  println(s"Registered 'BDSS01_Iguzman_enriched_trip_value' schema with ID ")
}
