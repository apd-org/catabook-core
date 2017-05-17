package com.cuandas.catabook.backend

import com.cuandas.catabook.schema.{AccessLevel, Dataset}
import com.cuandas.catabook.utils.EfficiencyUtils
import com.cuandas.catabook.CataConf
import org.scalatest.FunSuite

class BackendSuite extends FunSuite {

  val conf = new CataConf()
  val testset1 = createDataset("test-data1")
  val testset2 = createDataset("test-data2")
  val testset3 = createDataset("test-data3")

  private def createDataset(id: String): Dataset = {
    Dataset(
      identifier = id,
      title = "test dataset",
      description = "A temporay dataset for test purpose",
      keyword = List("demo", "simple1"),
      publisher = null,
      contactPoint = null,
      accessLevel = AccessLevel.PUBLIC
    )
  }

  ignore("Save a dataset to mongodb") {
    val cls = "com.cuandas.catabook.backend.MongoBackend"
    val backend = EfficiencyUtils.instantiate[SinkBackend](cls)(conf)
    backend.initialize()
    backend.saveDataset(testset1)
    Thread.sleep(3000)
    backend.stop()
  }

  test("Mysql backend") {
    val cls = "com.cuandas.catabook.backend.MysqlBackend"
    val mysql = EfficiencyUtils.instantiate[SinkBackend](cls)(conf)
    mysql.initialize()
    mysql.stop()
  }
}
