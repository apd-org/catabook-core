package com.cuandas.catabook.schema

import com.cuandas.catabook.schema.AccessLevel.AccessLevel
import com.mongodb.casbah.commons.MongoDBObject
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.JsonDSL._
import com.mongodb.casbah.Imports._

/**
  * Dataset metadata
  */
case class Dataset
(
  // Required
  var identifier: String,
  var title: String,
  var description: String,
  var keyword: List[String],
  var accessLevel: AccessLevel,
  // Optional
  var catalogId: String = null,
  var name: String = null,
  var created: String = null,
  var modified: String = null,
  var license: String = null,
  var rights: String = null,
  var spatial: String = null,
  var temporal: String = null,
  var accrualPeriodicity: String = null,
  var conformsTo: String = null,
  var dataQuality: Boolean = false,
  var describedBy: String = null,
  var describedByType: String = null,
  var isPartOf: String = null,
  var issued: String = null,
  var landingPage: String = null,
  var primaryITInvestmentUII: String = null,
  var systemOfRecords: String = null,
  var bureauCode: List[String] = List.empty,
  var programCode: List[String] = List.empty,
  var language: List[String] = List.empty,
  var references: List[String] = List.empty,
  var theme: List[Theme] = List.empty,
  var publisher: Publisher,
  var contactPoint: ContactPoint,
  var distribution: List[Distribution] = List.empty
)
  extends SchemaCommon("dcat:Dataset") with Serializable {

  override implicit def convertJObject[Dataset](self: Dataset): JObject = {
    ("@type" -> type_) ~
      ("title" -> title) ~
      ("description" -> description) ~
      ("keyword" -> keyword.map(JString)) ~
      ("publisher" -> Option(publisher).map(_.convertJObject()).orNull) ~
      ("contactPoint" -> Option(contactPoint).map(_.convertJObject()).orNull) ~
      ("identifier" -> identifier) ~
      ("accessLevel" -> Option(accessLevel).map(_.toString).orNull) ~
      ("catalogId" -> catalogId) ~
      ("name" -> name) ~
      ("created" -> created) ~
      ("modified" -> modified) ~
      ("bureauCode" -> bureauCode.map(JString)) ~
      ("programCode" -> programCode.map(JString)) ~
      ("license" -> license) ~
      ("rights" -> rights) ~
      ("spatial" -> spatial) ~
      ("temporal" -> temporal) ~
      ("accrualPeriodicity" -> accrualPeriodicity) ~
      ("conformsTo" -> conformsTo) ~
      ("dataQuality" -> dataQuality) ~
      ("describedBy" -> describedBy) ~
      ("describedByType" -> describedByType) ~
      ("isPartOf" -> isPartOf) ~
      ("issued" -> issued) ~
      ("language" -> language.map(JString)) ~
      ("landingPage" -> landingPage) ~
      ("primaryITInvestmentUII" -> primaryITInvestmentUII) ~
      ("systemOfRecords" -> systemOfRecords) ~
      ("references" -> references.map(JString)) ~
      ("distribution" -> distribution.map(_.convertJObject())) ~
      ("theme" -> theme.map(_.convertJObject()))
  }

  override implicit def convertMongoDBObject[Dataset](self: Dataset): MongoDBObject = {
    MongoDBObject(
      "@type" -> type_,
      "title" -> title,
      "description" -> description,
      "keyword" -> keyword,
      "publisher" -> Option(publisher).map(_.convertMongoDBObject()).orNull,
      "contactPoint" -> Option(contactPoint).map(_.convertMongoDBObject()).orNull,
      "identifier" -> identifier,
      "accessLevel" -> Option(accessLevel).map(_.toString).orNull,
      "catalogId" -> catalogId,
      "name" -> name,
      "created" -> created,
      "modified" -> modified,
      "bureauCode" -> bureauCode,
      "programCode" -> programCode,
      "license" -> license,
      "rights" -> rights,
      "spatial" -> spatial,
      "temporal" -> temporal,
      "accrualPeriodicity" -> accrualPeriodicity,
      "conformsTo" -> conformsTo,
      "dataQuality" -> dataQuality,
      "describedBy" -> describedBy,
      "describedByType" -> describedByType,
      "isPartOf" -> isPartOf,
      "issued" -> issued,
      "language" -> language,
      "landingPage" -> landingPage,
      "primaryITInvestmentUII" -> primaryITInvestmentUII,
      "systemOfRecords" -> systemOfRecords,
      "references" -> references,
      "distribution" -> distribution.map(_.convertMongoDBObject()),
      "theme" -> theme.map(_.convertMongoDBObject())
    )
  }
}
