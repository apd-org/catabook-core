package com.cuandas.catabook.sources

import java.io.{FileInputStream, InputStream}
import java.util
import java.util.Properties
import java.util.concurrent.{Callable, ExecutorCompletionService, Executors, Future}

import com.cuandas.catabook.utils.EfficiencyUtils
import com.cuandas.catabook.{CataContext, Logging, SourceConfigException}
import com.cuandas.catabook.{CataConf, CataContext, Logging}
import com.mongodb.internal.thread.DaemonThreadFactory

import scala.collection.JavaConversions._
import scala.xml.XML

/**
  * This class holds all plugined data sources.
  */
class SourceManager(conf: CataConf) extends Logging {

  private val DATA_SOURCES = "datasources"
  private val DATA_SOURCE = "datasource"
  private val SOURCE_ID = "id"
  private val SOURCE_NAME = "name"
  private val SOURCE_DESC = "description"
  private val SOURCE_CLASS = "class"
  private val SOURCE_PROPS = "properties"
  private val SOURCE_ENABLED = "enabled"

  private val sources: util.HashMap[String, DataSource] = new util.HashMap()
  private var context: CataContext = _

  // Read configuration file as input stream
  private val configIs: Option[InputStream] = {
    var is: InputStream = null
    val sourceConfig = conf.get(CataConf.DATA_SOURCE_CONFIG, null)
    if (sourceConfig != null) {
      is = new FileInputStream(sourceConfig)
    } else {
      is = getClass.getResourceAsStream("/datasources.xml")
      if (is == null) {
        throw new RuntimeException(s"There is no file 'datasources.xml' found")
      }
    }
    Option(is)
  }

  // Load sources from configurations
  configIs.foreach{ is =>
    loadDataSources(is).foreach { source =>
      if (sources.containsKey(source.id)) {
        throw SourceConfigException(s"Duplicated sources with the same id: '${source.id}'")
      } else {
        sources.put(source.id, source)
      }
    }
  }

  /** Load xml-formatted source configurations into a list of data pages */
  private[sources] def loadDataSources(source: InputStream): Seq[DataSource] = {
    val xml = XML.load(source)

    (xml \\ DATA_SOURCES \ DATA_SOURCE).map{ source => {
      val sourceId = (source \ SOURCE_ID).text
      val sourceName = (source \ SOURCE_NAME).text
      val sourceDesc = (source \ SOURCE_DESC).text
      val driverName = (source \ SOURCE_CLASS).text
      val enabled = (source \ SOURCE_ENABLED).text != "false"

      val properties: Properties = new Properties()
      (source \ SOURCE_PROPS \ "_") foreach { property =>
        properties.put(property.label, property.text)
      }

      EfficiencyUtils.instantiate[DataSource](driverName)(
        sourceId,
        sourceName,
        driverName,
        sourceDesc,
        properties,
        enabled.asInstanceOf[AnyRef]
      )
    }}
  }

  def setContext(context: CataContext) = this.context = context
  def size: Int = sources.size
  def getSources: Array[DataSource] = sources.values().toSeq.toArray
  def getSourcesEnabled: Array[DataSource] = getSources.filter { _.enabled }

  private case class TaskDesc(sourceId: String, taskId: Int, identifiers: List[String])
  private case class TaskResult(sourceId: String, taskId: Int, successful: Boolean)

  private val poolSize = conf.getInt("catabook.source.pool.size", 5)
  // Speed up data retrieving by paralleling
  private val requestsPerTask = conf.getInt("catabook.source.requests.per.task", 10)
  private val threadPool = Executors.newFixedThreadPool(poolSize,
    new DaemonThreadFactory("data-source"))
  private val pool = new ExecutorCompletionService[TaskResult](threadPool)
  private val futures = new util.LinkedList[Future[TaskResult]]()

  /**
    * split and start source tasks
    */
  def startSourcesEnabled(): Unit = {
    getSourcesEnabled.foreach { source =>
      val catalogId = source.catalogId
      val existing = context.backend.getExistingDatasetIds(catalogId)
      logInfo(s"Existing ${existing.size} datasets found for source [${source.id}]")
      val unparsed = source.getDatasetIds(ignored = existing)
      val taskNum = Math.ceil(1.0 * unparsed.size / requestsPerTask).toInt
      for (part <- 0 until taskNum) {
        val ids = unparsed.slice(part * requestsPerTask, (part+1) * requestsPerTask)
        val task = TaskDesc(source.id, part, ids)
        val taskCallable = new Callable[TaskResult] {
          override def call(): TaskResult = {
            try {
              val total = task.identifiers.size
              var parsed = 0
              val datasetIter = source.getDatasets(task.identifiers)
              while (datasetIter.hasNext) {
                val dataset = datasetIter.next()
                parsed += 1
                if (dataset != null) {
                  context.backend.saveDataset(dataset)
                  Thread.sleep(1000)
                }
                logInfo(s"Source [${task.sourceId}] - task ${task.taskId}/${taskNum} - query ${parsed}/${total}")
              }
              TaskResult(task.sourceId, task.taskId, true)
            } catch {
              case e: Exception =>
                logError((e.toString :: e.getStackTrace.map(_.toString) :: Nil).mkString("\n"))
                TaskResult(task.sourceId, task.taskId, false)
            }
          }
        }
        val future = pool.submit(taskCallable)
        futures.append(future)
      }
      logInfo(s"Total ${taskNum} tasks submitted for source [${source.id}]")
    }

    // stop to accept more tasks
    threadPool.shutdown()
  }

  def awaitFinish() = {
    try {
      futures.foreach { f =>
        // check running result
        val res = f.get()
        if (res.successful) {
          logInfo(s"Successfully finish task ${res.taskId} for source [${res.sourceId}]")
        } else {
          logWarning(s"Failed task ${res.taskId} for source [${res.sourceId}]")
        }
      }
    } catch {
      case e: InterruptedException =>
        logWarning("Interruption encountered, stop all source workers.")
        threadPool.shutdownNow()
      case e: Exception => throw e
    }
  }
}
