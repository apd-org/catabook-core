package com.cuandas.catabook.backend

import com.cuandas.catabook.schema.AccessLevel.AccessLevel
import com.cuandas.catabook.schema._
import com.cuandas.catabook.CataConf

abstract class SinkBackend(conf: CataConf) {

  /** Start the backend */
  def initialize(): Unit

  /** Stop the backend and clear environment */
  def stop(): Unit

  /** Get dataset of given identifier */
  def getDataset(datasetId: String): Option[Dataset]

  /** Save dataset into backend */
  def saveDataset(dataset: Dataset, replaceable: Boolean = true): Unit

  /** Return a list of existing datasets */
  def getDatasets(catalogId: String): List[Dataset]

  /** Return a list of existing datasets identifiers */
  def getExistingDatasetIds(catalogId: String): List[String]

  /** Return a list of dataset distributions */
  def getDistributions(datasetId: String): List[Distribution]

  /** Return an Option of publisher */
  def getPublisher(datasetId: String): Option[Publisher]

  /** Return an Option of ContactPoint */
  def getContactPoint(datasetId: String): Option[ContactPoint]

  /** Return a list of themes */
  def getThemes(datasteId: String): List[Theme]

  /** Return the catalogId of specific dataset */
  def getCatalog(datasetId: String): Option[String]

  /** Return the accessLevel of specific dataset */
  def getAccessLevel(datasetId: String): AccessLevel
}
