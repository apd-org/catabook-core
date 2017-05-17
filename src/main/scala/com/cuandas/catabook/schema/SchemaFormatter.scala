package com.cuandas.catabook.schema

/**
  * Interface to exchange information between schema standards
  */
abstract class SchemaFormatter {
  /**
    * Convert a string that is source-specific into Dataset instance
    */
  def formatAsDataset(dataset: String): Dataset
}
