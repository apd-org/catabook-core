package com.cuandas.catabook.schema

import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._

/**
  * Dataset maintainer
  */
case class ContactPoint(fn: String, hasEmail: String) extends
  SchemaCommon("vcard:Contact") with Serializable {

  override implicit def convertJObject[ContactPoint](self: ContactPoint): JObject = {
    ("@type" -> type_) ~
      ("fn" -> fn) ~
      ("hasEmail" -> hasEmail)
  }

  override implicit def convertMongoDBObject[ContactPoint](self: ContactPoint): MongoDBObject = {
    MongoDBObject(
      "@type" -> type_,
      "fn" -> fn,
      "hasEmail" -> hasEmail
    )
  }
}
