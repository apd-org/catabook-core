package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._


/** Dataset distribution info. */
case class Distribution
(
  var format: String = null,
  var mediaType: String = null,
  var accessURL: String = null,
  var downloadURL: String = null,
  var conformsTo: String = null,
  var describedBy: String = null,
  var describedByType: String = null,
  var description: String = null,
  var title: String = null
)
  extends SchemaCommon("dcat:Distribution") with Serializable {

  override implicit def convertJObject[Distribution](self: Distribution): JObject = {
    ("@type" -> type_) ~
      ("format" -> format) ~
      ("mediaType" -> mediaType) ~
      ("accessURL" -> accessURL) ~
      ("downloadURL" -> downloadURL) ~
      ("conformsTo" -> conformsTo) ~
      ("describedBy" -> describedBy) ~
      ("describedByType" -> describedByType) ~
      ("description" -> description) ~
      ("title" -> title)
  }

  override implicit def convertMongoDBObject[Distribution](self: Distribution): MongoDBObject = {
    MongoDBObject(
      "@type" -> type_,
      "format" -> format,
      "mediaType" -> mediaType,
      "accessURL" -> accessURL,
      "downloadURL" -> downloadURL,
      "conformsTo" -> conformsTo,
      "describedBy" -> describedBy,
      "describedByType" -> describedByType,
      "description" -> description,
      "title" -> title
    )
  }
}
