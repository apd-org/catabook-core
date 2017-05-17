package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._

/**
  * A collection of datasets for a agency
  */
case class Catalog
(
  var id_ : String = null,
  var context_ : String = Schema.POD_SCHEMA_V11_CONTEXT,
  var conformsTo: String = Schema.POD_SCHEMA_V11,
  var describedBy: String = Schema.POD_SCHEMA_V11_CATALOG,
  var datasets: List[Dataset] = List.empty[Dataset]
)
  extends SchemaCommon("dcat:Catalog") with Serializable {

  override implicit def convertJObject[Catalog](self: Catalog): JObject = {
    ("@type" -> type_) ~
      ("@id" -> id_) ~
      ("@context" -> context_) ~
      ("conformsTo" -> conformsTo) ~
      ("describedBy" -> describedBy) ~
      ("datasets" -> datasets.map(x => x.convertJObject()))
  }

  override implicit def convertMongoDBObject[Catalog](self: Catalog): MongoDBObject = {
    MongoDBObject(
      "@type" -> type_,
      "@id" -> id_,
      "@context" -> context_,
      "conformsTo" -> conformsTo,
      "describedBy" -> describedBy,
      "datasets" -> datasets.map(_.convertMongoDBObject())
    )
  }
}
