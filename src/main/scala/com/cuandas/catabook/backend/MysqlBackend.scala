package com.cuandas.catabook.backend

import java.sql.{Connection, DriverManager, Statement}

import com.cuandas.catabook.schema.AccessLevel.AccessLevel
import com.cuandas.catabook.schema._
import com.cuandas.catabook.BackendConfigException
import com.cuandas.catabook.CataConf

import scala.util.matching.Regex

/**
  * Created by chenxm on 28/03/2017.
  */
class MysqlBackend(conf: CataConf) extends SinkBackend(conf) {

  private val jdbcAddr = conf.get("catabook.jdbc.mysql", null)
  private var dbName = "catabook"
  private val tblDataset = "datasets"

  var con: Connection = null
  var stmt: Statement = null

  /** Start the backend */
  override def initialize(): Unit = {
    if (jdbcAddr == null || jdbcAddr.isEmpty)
      throw BackendConfigException("Invalid JDBC address. Please set 'catabook.jdbc.mysql' properly.")

    // identify custom database name
    val regex = new Regex("jdbc:mysql://([^:/]+):(\\d+)/([^/]+)")
    val m = regex.findFirstMatchIn(jdbcAddr)
    if (m.isDefined) {
      dbName = m.get.group(2)
    }

    try {
      con = DriverManager.getConnection(jdbcAddr)
      stmt = con.createStatement()
      Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
      initializeDatabase()
    } finally {
      if (stmt != null)
        stmt.close()
      if (con != null)
        con.close()
    }
  }

  /** Stop the backend and clear environment */
  override def stop(): Unit = {}

  /** Get dataset of given identifier */
  override def getDataset(datasetId: String): Option[Dataset] = None

  private def initializeDatabase(): Unit = {
    // create database
    stmt.executeUpdate(s"CREATE DATABASE IF NOT EXISTS ${dbName} " +
      s"DEFAULT CHARSET 'utf8' COLLATE 'utf8_general_ci'")

    // dataset

    // contact

    // publisher

    // theme

    // distribution
  }

  /** Save dataset into backend */
  override def saveDataset(dataset: Dataset, replaceable: Boolean): Unit = ???

  /** Return a list of existing datasets */
  override def getDatasets(catalogId: String): List[Dataset] = ???

  /** Return a list of existing datasets identifiers */
  override def getExistingDatasetIds(catalogId: String): List[String] = ???

  /** Return a list of dataset distributions */
  override def getDistributions(datasetId: String): List[Distribution] = ???

  /** Return an Option of publisher */
  override def getPublisher(datasetId: String): Option[Publisher] = ???

  /** Return an Option of ContactPoint */
  override def getContactPoint(datasetId: String): Option[ContactPoint] = ???

  /** Return a list of themes */
  override def getThemes(datasteId: String): List[Theme] = ???

  /** Return the catalogId of specific dataset */
  override def getCatalog(datasetId: String): Option[String] = ???

  /** Return the accessLevel of specific dataset */
  override def getAccessLevel(datasetId: String): AccessLevel = ???
}
