package SpayJson

import spray.json.DefaultJsonProtocol.{IntJsonFormat, listFormat}
import spray.json._

object main extends App {

  val source = scala.io.Source.fromFile("/Users/marinda/Documents/Studies/1MCIT/Data/bixi_data/system_information.json")
  val lines = try source.mkString finally source.close()
  println(lines)
  val yourJson: JsValue = lines.parseJson
  println(yourJson)

  val json = yourJson.prettyPrint // or .compactPrint
  println(json)

  val jsonAst = List(1, 2, 3).toJson
println(jsonAst)
}
