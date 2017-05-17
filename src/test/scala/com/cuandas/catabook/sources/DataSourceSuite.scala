package com.cuandas.catabook.sources

import java.io.InputStream

import com.cuandas.catabook.sources.ckan.CkanSource
import com.cuandas.catabook.CataConf
import org.apache.commons.io.IOUtils
import org.scalatest.FunSuite

class DataSourceSuite extends FunSuite {

  test("DataPage object conversion") {
    val sourceConfig = """<datasources>
                         |    <datasource>
                         |        <id>datahub.io</id>
                         |        <name>Datahub</name>
                         |        <description>Datasets on datahub, which a website based on CKAN project.</description>
                         |        <class>com.cuandas.catabook.sources.ckan.CkanSource</class>
                         |        <properties>
                         |            <host>https://datahub.io</host>
                         |            <api.version>3</api.version>
                         |        </properties>
                         |        <enabled>true</enabled>
                         |    </datasource>
                         |</datasources>"""
    val manager = new SourceManager(new CataConf())
    val loadDataSources = classOf[SourceManager].getDeclaredMethod("loadDataSources", classOf[InputStream])
    loadDataSources.setAccessible(true)
    val sources = loadDataSources.invoke(manager, IOUtils.toInputStream(sourceConfig))
      .asInstanceOf[Seq[DataSource]]

    sources.foreach { page => {
      val ckan = page.asInstanceOf[CkanSource]
      assert(ckan.id == "datahub.io")
      assert(ckan.driverName == "com.cuandas.catabook.sources.ckan.CkanSource")
      assert(ckan.properties.getProperty("host") == "https://datahub.io")
      assert(ckan.properties.getProperty("api.version") == "3")
    }}
  }
}
