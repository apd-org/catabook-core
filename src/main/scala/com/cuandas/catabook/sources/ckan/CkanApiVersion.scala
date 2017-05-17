package com.cuandas.catabook.sources.ckan

/**
  * Created by chenxm on 17-2-18.
  */
sealed trait CkanApiVersion

case object CKAN_API_V1 extends CkanApiVersion

case object CKAN_API_V2 extends CkanApiVersion

case object CKAN_API_V3 extends CkanApiVersion
