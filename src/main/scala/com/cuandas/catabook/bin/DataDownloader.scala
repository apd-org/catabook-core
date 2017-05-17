package com.cuandas.catabook.bin

import com.cuandas.catabook.{CataConf, CataContext, Logging}

/**
  * Main portal to download data sources
  */
object DataDownloader extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new CataConf()
    val context = new CataContext(conf)
    context.manager.startSourcesEnabled()
    context.manager.awaitFinish()
  }
}
