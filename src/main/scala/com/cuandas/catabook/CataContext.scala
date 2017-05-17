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

import com.cuandas.catabook.backend.SinkBackend
import com.cuandas.catabook.sources.SourceManager
import com.cuandas.catabook.utils.EfficiencyUtils

/**
  * Global context info for Catabook
  */
class CataContext(conf: CataConf) extends Logging {

  logInfo("Initializing Catabook context")

  // create source manager
  val manager = new SourceManager(conf)
  logInfo(s"Totally ${manager.getSources.count(_.enabled)}/${manager.size}" +
    s" data sources loaded.")
  manager.setContext(this)

  // create data backend
  private val backendClass = conf.get("catabook.backend", "com.cuandas.catabook.backend.ConsoleBackend")
  val backend = EfficiencyUtils.instantiate[SinkBackend](backendClass)(conf)
  backend.initialize()

  def clear(): Unit = {
    logInfo("Clearing Catabook context")
  }
}
