package com.cuandas.catabook.schema

import org.scalatest.FunSuite

/**
  * Created by chenxm on 20/02/2017.
  */
class SchemaSuite extends FunSuite {

  test("distribution schema") {
    val dist = Distribution(
      format = "CSV",
      mediaType = "zip",
      title = "test dist.",
      accessURL = "http://example.com/dist"
    )
    println(dist.toJson)
  }

  test("publisher schema") {
    val pub = Publisher(name = "pub1", subOrganizationOf = Publisher("pub0", null))
    println(pub.toJson)
  }

  test("dataset schema") {
    val dat = Dataset(
      identifier = "data1",
      title = "test dataset",
      description = "A temporay dataset for test purpose",
      keyword = List("demo", "simple1"),
      publisher = Publisher(name = "pub1", subOrganizationOf = null),
      contactPoint = null,
      accessLevel = AccessLevel.PUBLIC
    )
    println(dat.convertMongoDBObject())
  }

}
