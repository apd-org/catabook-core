package com.cuandas.catabook.sources.ckan

import java.util.Properties

import com.cuandas.catabook.Logging
import org.scalatest.FunSuite

import scala.util.Random

/**
  * Created by chenxm on 17-2-8.
  */
class CkanSourceSuite extends FunSuite with Logging {

  private var packages: Seq[String] = Seq.empty[String]
  val props = new Properties()
  props.setProperty("host", "http://demo.ckan.org")
  props.setProperty("api.version", "3")
  private val ckan = new CkanSource(
    "test",
    "test",
    "com.cuandas.catabook.sources.ckan.CKAN",
    "",
    props
  )

  private def fetchDatasetNames = {
    val m = classOf[CkanSource].getDeclaredMethod("fetchDatasetNames", classOf[CkanApiVersion])
    m.setAccessible(true)
    m
  }

  private def fetchSingleDataset = {
    val m = classOf[CkanSource].getDeclaredMethod("fetchSingleDataset", classOf[String], classOf[CkanApiVersion])
    m.setAccessible(true)
    m
  }

  test("list all packages") {
    packages = fetchDatasetNames.invoke(ckan, CKAN_API_V3).asInstanceOf[Seq[String]]
    logInfo(s"Total ${packages.size} packages")
  }

  ignore("fetch single package one by one randomly") {
    randomPackage.foreach { id =>
      val dataset = fetchSingleDataset.invoke(ckan, id).asInstanceOf[String]
      logInfo(s"Package details: ${dataset}")
    }
  }

  ignore("transform ckan sources to dataset") {
    randomPackage.foreach { id =>
      val dataset = fetchSingleDataset.invoke(ckan, id).asInstanceOf[String]
      val transformer = new CkanFormatter(CKAN_API_V3)
      transformer.formatAsDataset(dataset)
    }
  }

  ignore("fetch dataset with groups exception") {
    for (v <- List(CKAN_API_V1, CKAN_API_V2, CKAN_API_V3)) {
      val ds = fetchSingleDataset.invoke(ckan, "123456", v).asInstanceOf[String]
      val transformer = new CkanFormatter(v)
      transformer.formatAsDataset(ds)
    }
  }

  ignore("fetch dataset with tags exception") {
    for (v <- List(CKAN_API_V1, CKAN_API_V2, CKAN_API_V3)) {
      val ds = fetchSingleDataset.invoke(ckan, "100", v).asInstanceOf[String]
      val transformer = new CkanFormatter(v)
      transformer.formatAsDataset(ds)
    }
  }

  private def randomPackage: Option[String] = {
    val len = packages.length
    if (len > 0) {
      val index = Random.nextInt(len)
      Some(packages(index))
    } else {
      None
    }
  }
}
