package com.cuandas.catabook.utils

object EfficiencyUtils {

  def instantiate[T](clazz: java.lang.Class[T])(args:AnyRef*): T = {
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(args:_*).asInstanceOf[T]
  }

  def instantiate[T](className: String)(args: AnyRef*): T = {
    val clazz = Class.forName(className)
    val constructor = clazz.getConstructors()(0)
    constructor.newInstance(args: _*).asInstanceOf[T]
  }
}
