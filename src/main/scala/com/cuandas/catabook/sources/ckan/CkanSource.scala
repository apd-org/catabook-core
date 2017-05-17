package com.cuandas.catabook.sources.ckan

import java.util.Properties

import com.cuandas.catabook.{Logging, SourceConfigException}
import com.cuandas.catabook.schema.Dataset
import com.cuandas.catabook.sources.DataSource
import org.apache.commons.io.IOUtils
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.{ClientProtocolException, ResponseHandler}
import org.apache.http.impl.client.HttpClients
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.{DefaultFormats, JArray, JValue}

/**
  * Implement data source of CKAN
  */
class CkanSource
(
  id: String = "",
  name: String = null,
  driverName: String = null,
  description: String = null,
  properties: Properties = new Properties(),
  enabled: Boolean = true
) extends DataSource(id, name, driverName, description, properties, enabled)
  with Logging {

  protected implicit val formats = DefaultFormats

  private val apiVersion = this.properties.getProperty("api.version", null) match {
    case "1" => CKAN_API_V1
    case "2" => CKAN_API_V2
    case _ => CKAN_API_V3
  }

  private val formatter = new CkanFormatter(apiVersion)
  private val httpClient = HttpClients.createDefault()

  private var host = this.properties.getProperty("host", null)
  if (host == null) {
    throw SourceConfigException("The 'host' property can not be empty for CAKN source")
  } else {
    if (!host.startsWith("http://") && !host.startsWith("https://"))
      host = "http://" + host
  }

  override def getDatasetIds(ignored: List[String] = List.empty): List[String] = {
    fetchDatasetNames().filter(!ignored.contains(_)).toList
  }

  override def getDatasets(identifiers: List[String]): Iterator[Dataset] = {
    for {
      p <- identifiers.toIterator
      json = fetchSingleDataset(p)
    } yield {
      // convert to backend dataset instance
      val dataset = formatter.formatAsDataset(json)
      dataset.catalogId = this.id
      genGlobalDatasetId(dataset)
      // FIXME: check distribution link address
      def fixAccessUrl(url: String): String = {
        url.replaceFirst("^[:/]+", this.host.stripSuffix("/") + "/")
      }
      for (dist <- dataset.distribution) {
        dist.accessURL = fixAccessUrl(dist.accessURL)
        dist.downloadURL = fixAccessUrl(dist.downloadURL)
      }
      dataset
    }
  }

  /* Get a list of dataset names */
  private def fetchDatasetNames(version: CkanApiVersion = apiVersion): Seq[String] = {
    val packages = version match {
      case CKAN_API_V1 => listPackageV1()
      case CKAN_API_V2 => listPackageV2()
      case CKAN_API_V3 => listPackageV3()
      case _ => listPackageV3()
    }
    assert(packages.isInstanceOf[JArray])
    packages.asInstanceOf[JArray].arr
      .map(_.extract[String])
  }

  /* Get detailed info. of specific dataset */
  private def fetchSingleDataset(dataset: String,
                                 version: CkanApiVersion = apiVersion): String = {
    logDebug(s"Fetching dataset ${dataset} (${version})")
    val packageInfo = version match {
      case CKAN_API_V1 => fetchPackageV1(dataset)
      case CKAN_API_V2 => fetchPackageV2(dataset)
      case CKAN_API_V3 => fetchPackageV3(dataset)
      case _ => fetchPackageV3(dataset)
    }
    compact(render(packageInfo))
  }

  private def listPackageV1(): JValue = parse(doRequest(host + "/api/1/rest/dataset"))

  private def listPackageV2(): JValue = parse(doRequest(host + "/api/2/rest/dataset"))

  private def listPackageV3(): JValue = parse(doRequest(host + "/api/3/action/package_list")) \ "result"

  private def fetchPackageV1(identifier: String): JValue = parse(doRequest(host + s"/api/1/rest/dataset/${identifier}"))

  private def fetchPackageV2(identifier: String): JValue = parse(doRequest(host + s"/api/2/rest/dataset/${identifier}"))

  private def fetchPackageV3(identifier: String): JValue = parse(doRequest(host + s"/api/3/action/package_show?id=${identifier}")) \ "result"

  private def doRequest(website: String): String = {
    val request = new HttpGet(website)
    request.setHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36")
    request.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
    request.setHeader("Accept-Encoding", "gzip, deflate, sdch, br")

    httpClient.execute(request, new ResponseHandler[String]() {
      override def handleResponse(httpResponse: HttpResponse): String = {
        var responseBody = ""
        val status = httpResponse.getStatusLine.getStatusCode
        if (status >= 200 && status < 300) {
          val entity = httpResponse.getEntity
          if (entity != null)
            responseBody = IOUtils.toString(entity.getContent)
        } else {
          throw new ClientProtocolException(s"Unexpected response status: ${status}")
        }
        responseBody
      }
    })
  }
}
