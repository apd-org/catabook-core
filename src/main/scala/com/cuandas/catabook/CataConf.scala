/*
  Copyright 2016 Catabook Group

  Licensed under the Apache License, Version 2.0 (the "License"); you may not
  use this file except in compliance with the License. You may obtain a copy of
  the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations under
  the License.
 */
package com.cuandas.catabook

import java.io.{FileInputStream, InputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.xml.XML

/**
  * Global configurations of Catabook.
  */
class CataConf(loadDefaults: Boolean) extends Cloneable with Logging {

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    for ((k, v) <- System.getProperties.asScala if k.startsWith("catabook.")) {
      set(k, v)
    }
  }

  loadCatabookConfig()

  def this() = this(true)

  /**
    * Read XML-based catabook configurations which are pairs
    * of property and value.
    */
  private def loadCatabookConfig(): Unit = {
    var is: InputStream = null
    val globalConfig = System.getProperty(CataConf.CATABOOK_CONFIG, null)
    if (globalConfig != null) {
      is = new FileInputStream(globalConfig)
    } else {
      is = getClass.getResourceAsStream("/catabook.xml")
    }

    if (is != null) {
      val xml = XML.load(is)
      (xml \\ "configurations" \ "property").foreach {property =>
        val name = (property \ "name").text
        val value = (property \ "value").text
        set(name, value)
      }
    } else {
      logWarning(s"Configuration file 'catabook.xml' not found or " +
        s"'${CataConf.CATABOOK_CONFIG}' not set")
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): CataConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings.put(key, value)
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings.putAll(settings.toMap.asJava)
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): CataConf = {
    settings.putIfAbsent(key, value)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): CataConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(key)

  /** Copy this object */
  override def clone: CataConf = {
    new CataConf(false).setAll(getAll)
  }
}

object CataConf {

  /* Configuration keys */
  lazy val CATABOOK_CONFIG = "catabook.config.file"
  lazy val DATA_SOURCE_CONFIG = "catabook.datasources.file"

  /* Default values */

}
