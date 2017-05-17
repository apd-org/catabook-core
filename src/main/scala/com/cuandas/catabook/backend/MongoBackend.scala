package com.cuandas.catabook.backend

import com.cuandas.catabook.schema.AccessLevel.AccessLevel
import com.cuandas.catabook.schema._
import com.cuandas.catabook.Logging
import com.cuandas.catabook.{CataConf, Logging}
import com.mongodb.casbah.Imports._

/**
  * Store backend based on mongodb
  */
class MongoBackend(conf: CataConf) extends SinkBackend(conf) with Logging {

  private val database = conf.get("catabook.mongodb.auth.db", "catabook")

  private val mongoClient = {
    var server = conf.get("catabook.mongodb.server", "localhost:27017")
    if (server.startsWith("mongodb://"))
      server = server.substring(10)
    val serverAddress = new ServerAddress(server)
    val username = conf.get("catabook.mongodb.auth.user", null)
    val password = conf.get("catabook.mongodb.auth.passwd", null)
    if (username != null) {
      val credentials = MongoCredential.createMongoCRCredential(username, database, password.toCharArray)
      MongoClient(serverAddress, List(credentials))
    } else {
      MongoClient(serverAddress)
    }
  }

  private val db = mongoClient.apply(database)
  private val dataset = db.apply("dataset")

  override def initialize(): Unit = {}

  override def stop(): Unit = {
    mongoClient.close()
  }

  override def getDataset(datasetId: String): Option[Dataset] = {
    val res = dataset.findOne(MongoDBObject("identifier" -> datasetId))
    res.map { i =>
      (new JsonFormatter).formatAsDataset(i.toString)
    }
  }

  override def saveDataset(newDataset: Dataset,
                           replaceable: Boolean = true): Unit = synchronized {
    val id = newDataset.identifier
    if (replaceable) {
      this.dataset.remove(MongoDBObject("identifier" -> id))
    }
    this.dataset.insert(newDataset.convertMongoDBObject())
  }

  /** Return a list of existing datasets */
  override def getDatasets(catalogId: String): List[Dataset] = {
    val jsonFormatter = new JsonFormatter
    val cur = this.dataset.find(MongoDBObject("catalogId" -> catalogId))
    cur.map { doc =>
      jsonFormatter.formatAsDataset(doc.toString)
    }.toList
  }

  /** Return a list of existing datasets identifiers */
  override def getExistingDatasetIds(catalogId: String): List[String] = {
    val cur = this.dataset.find(
      MongoDBObject("catalogId" -> catalogId),
      MongoDBObject("identifier" -> 1)
    )
    cur.map { _.get("identifier").asInstanceOf[String] }.toList
  }

  override def getDistributions(datasetId: String): List[Distribution] = {
    this.getDataset(datasetId).map { _.distribution }
      .getOrElse(List.empty)
  }

  override def getPublisher(datasetId: String): Option[Publisher] = {
    this.getDataset(datasetId).map { _.publisher }
  }

  override def getContactPoint(datasetId: String): Option[ContactPoint] = {
    this.getDataset(datasetId).map { _.contactPoint }
  }

  override def getThemes(datasetId: String): List[Theme] = {
    this.getDataset(datasetId).map { _.theme }
      .getOrElse(List.empty)
  }

  override def getCatalog(datasetId: String): Option[String] = {
    this.getDataset(datasetId).map { _.catalogId }
  }

  override def getAccessLevel(datasetId: String): AccessLevel = {
    this.getDataset(datasetId).map { _.accessLevel }.getOrElse(AccessLevel.NONE)
  }
}
