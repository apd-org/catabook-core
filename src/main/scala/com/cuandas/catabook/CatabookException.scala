package com.cuandas.catabook

/**
  * Created by chenxm on 17-2-8.
  */
abstract class AbstractCatabookException(msg: String, cause: Throwable = null)
  extends Exception(msg, cause)

case class CatabookException(msg: String, cause: Throwable = null)
  extends AbstractCatabookException(msg, cause)

case class SourceConfigException(msg: String, cause: Throwable = null)
  extends AbstractCatabookException(msg, cause)

case class BackendConfigException(msg: String, cause: Throwable = null)
  extends AbstractCatabookException(msg, cause)
