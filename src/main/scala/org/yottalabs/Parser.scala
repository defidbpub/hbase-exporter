package org.yottalabs

import java.io.StringReader
import io.circe.{Json, yaml}

object Parser extends App {

  def yamlParser(path: String): List[(String, String)] = {
    val ymlText = scala.io.Source.fromFile(path).mkString
    val js = yaml.parser.parse(new StringReader(ymlText))
    val json: Json = js.right.get
    val categories = (json \\ "CONF").flatMap(j => j.asObject.get.values.toList)
    val subs = categories.flatMap(j => j.asObject.get.toList)
    val elements: List[(String, String)] = subs.map { case (k, v) => (k, v.toString()) }
    elements
  }

}