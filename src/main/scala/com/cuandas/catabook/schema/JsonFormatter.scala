package com.cuandas.catabook.schema

import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * A formatter for json-style dataset and Dataset class
  */
class JsonFormatter extends SchemaFormatter {

  protected implicit val formats = DefaultFormats

  override def formatAsDataset(dataset: String): Dataset = {
    require(dataset != null && dataset.nonEmpty)

    val json = parse(dataset)
    val title = (json \ "title").extract[String]
    val description = (json \ "notes").extract[String]
    null
  }
}
