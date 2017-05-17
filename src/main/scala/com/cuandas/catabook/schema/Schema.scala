package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.native.JsonMethods._

abstract class SchemaCommon(val type_ : String) {
  def toJson: String = compact(render(this))
  implicit def convertJObject[T <: SchemaCommon](self: T): JObject
  implicit def convertMongoDBObject[T <: SchemaCommon](self: T): MongoDBObject
}

/**
  * Standalone utilities to process open data schema with Link Data specification.
  * This library is currently based on POD Schema v1.1.
  *
  * Reference: https://project-open-data.cio.gov/
  */
object Schema {
  val POD_SCHEMA_V11 = "https://project-open-data.cio.gov/v1.1/schema"
  val POD_SCHEMA_V11_CONTEXT = "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"
  val POD_SCHEMA_V11_CATALOG = "https://project-open-data.cio.gov/v1.1/schema/catalog.json"
}
