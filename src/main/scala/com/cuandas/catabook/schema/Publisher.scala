package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._


/** Dataset publishing organization or person */
case class Publisher(name: String, subOrganizationOf: Publisher) extends
  SchemaCommon("org:Organization") with Serializable {

  override implicit def convertJObject[Publisher](self: Publisher): JObject = {
    val json =
      ("@type" -> type_) ~
        ("name" -> name)
    if (subOrganizationOf != null)
      json ~ ("subOrganizationOf" -> subOrganizationOf.convertJObject())
    else
      json
  }

  override implicit def convertMongoDBObject[Publisher](self: Publisher): MongoDBObject = {
    val res = MongoDBObject(
      "@type" -> type_,
      "name" -> name
    )
    if (subOrganizationOf != null)
      res ++ ("subOrganizationOf" -> subOrganizationOf.convertMongoDBObject())
    else
      res
  }
}
