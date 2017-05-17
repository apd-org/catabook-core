package com.cuandas.catabook.backend

import com.cuandas.catabook.schema.AccessLevel.AccessLevel
import com.cuandas.catabook.Logging
import com.cuandas.catabook.schema._
import com.cuandas.catabook.{CataConf, Logging}

/**
  * A dummy class for testing purpose
  */
private[catabook] class ConsoleBackend(conf: CataConf) extends SinkBackend(conf) with Logging {
  /** Start the backend */
  override def initialize(): Unit = {
    logInfo(s"Starting backend ${getClass.getName}")
  }

  /** Stop the backend and clear environment */
  override def stop(): Unit = {
    logInfo(s"Stopping backend ${getClass.getName}")
  }

  /** Get dataset of given identifier */
  override def getDataset(datasetId: String): Option[Dataset] = None

  /** Save dataset into backend */
  override def saveDataset(dataset: Dataset, replaceable: Boolean = true): Unit = {
    logInfo(dataset.toJson)
  }

  /** Return a list of existing datasets */
  override def getDatasets(catalogId: String): List[Dataset] = List.empty

  /** Return a list of existing datasets identifiers */
  override def getExistingDatasetIds(catalogId: String): List[String] = List.empty

  override def getDistributions(datasetId: String): List[Distribution] = List.empty

  override def getPublisher(datasetId: String): Option[Publisher] = None

  override def getContactPoint(datasetId: String): Option[ContactPoint] = None

  override def getThemes(datasteId: String): List[Theme] = List.empty

  override def getCatalog(datasetId: String): Option[String] = None

  override def getAccessLevel(datasetId: String): AccessLevel = AccessLevel.PUBLIC
}
