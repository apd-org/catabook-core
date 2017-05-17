package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._

/** Dataset category or topic */
case class Theme(name: String, description: String) extends
  SchemaCommon("dcat:Theme") with Serializable {

  override implicit def convertJObject[Theme](self: Theme): JObject = {
    ("@type" -> type_) ~
      ("name" -> name) ~
      ("description" -> description)
  }

  override implicit def convertMongoDBObject[Theme](self: Theme): MongoDBObject = {
    MongoDBObject(
      "@type" -> type_,
      "name" -> name,
      "description" -> description
    )
  }
}
