package com.cuandas.catabook.sources

import java.util.{Properties, UUID}

import com.cuandas.catabook.schema.Dataset
import com.cuandas.catabook.utils.SecurityUtils

/**
  * An abstract class to interact with a data source.
  * The only action is to retrieve data from source.
  */
abstract class DataSource(val id: String, val name: String, val driverName: String,
                          val description: String, val properties: Properties,
                          val enabled: Boolean) {

  /** A data source is bound to a unique catalog */
  def catalogId: String = this.id

  /** Get a list of dataset identifiers by ignoring some entries. */
  def getDatasetIds(ignored: List[String]): List[String]

  /** Get a lazy iterator of all datasets with identifiers. */
  def getDatasets(identifiers: List[String]): Iterator[Dataset]

  /** Get a lazy iterator of all datasets by ignoring some entries. */
  def getDatasetsByIgnore(ignored: List[String] = List.empty[String]): Iterator[Dataset] = {
    getDatasets(getDatasetIds(ignored))
  }

  /** Generate global unique id for datasets of this source*/
  def genGlobalDatasetId(dataset: Dataset): Unit = {
    val sourceId = id
    val dataId = dataset.identifier
    val uuid = UUID.randomUUID()
    dataset.identifier = SecurityUtils.md5(sourceId + dataId) + "_" + uuid
  }
}
