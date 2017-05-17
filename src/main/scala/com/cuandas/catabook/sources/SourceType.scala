package com.cuandas.catabook.sources

/**
  * Enumeration of data source types
  */
object SourceType extends Enumeration {
  type SourceType = Value
  val NONE, DRIVER, REST = Value
}
