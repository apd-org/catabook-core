package com.cuandas.catabook.sources.ckan

import com.cuandas.catabook.schema._
import com.cuandas.catabook.Logging
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Transformer between CKAN restful result and schema
  */
private[ckan] class CkanFormatter(version: CkanApiVersion) extends JsonFormatter with Logging {

  override def formatAsDataset(datasetJson: String): Dataset = {
    if (datasetJson != null && !datasetJson.isEmpty) {
      logDebug(datasetJson)

      val json = parse(datasetJson)
      val title = (json \ "title").extract[String]
      val notes = (json \ "notes").extract[String]
      val tags = if (version == CKAN_API_V1 || version == CKAN_API_V2) {
        (json \ "tags").extract[List[String]]
      } else if (version == CKAN_API_V3) {
        (json \ "groups").extract[List[JObject]].map { jv => {
          (jv \ "name").extract[String]
        }}
      } else {
        List.empty[String]
      }

      val publisherName = (json \ "organization" \ "title").toOption.map {
        _.extract[String]
      }.orNull
      val maintainer = (json \ "maintainer").extract[String]
      val maintainerEmail = (json \ "maintainer_email").extract[String]
      val identifier = (json \ "id").extract[String]
      val name = (json \ "name").extract[String]
      val license = (json \ "license_id").extract[String]
      val created = (json \ "metadata_created").extract[String]
      val modified = (json \ "metadata_modified").extract[String]
      val publisher = Publisher(name = publisherName, subOrganizationOf = null)
      val distributions = (json \ "resources").extract[List[JObject]].map(toDistribution)
      val contactPoint = ContactPoint(fn = maintainer, hasEmail = maintainerEmail)

      val theme = if (version == CKAN_API_V1 || version == CKAN_API_V2) {
        (json \ "groups").extract[List[String]].map { name =>
          Theme(name = name, description = null)
        }
      } else if (version == CKAN_API_V3) {
        (json \ "groups").extract[List[JObject]].map { jv => {
          val name = (jv \ "name").extract[String]
          val description = (jv \ "description").extract[String]
          Theme(name = name, description = description)
        }}
      } else {
        List.empty[Theme]
      }

      Dataset(
        title = title,
        description = notes,
        keyword = tags,
        publisher = publisher,
        contactPoint = contactPoint,
        identifier = identifier,
        accessLevel = AccessLevel.PUBLIC,
        license = license,
        distribution = distributions,
        theme = theme,
        name = name,
        created = created,
        modified = modified
      )
    } else {
      null
    }
  }

  /* Convert CKAN resource to Distribution*/
  private def toDistribution(resource: JValue): Distribution = {
    val url = (resource \ "url").extract[String]
    val mimetype = (resource \ "mimetype").extract[String]
    val format = (resource \ "format").extract[String]
    val name = (resource \ "name").extract[String]
    val description = (resource \ "description").extract[String]

    Distribution(
      downloadURL = url,
      accessURL = url,
      mediaType = mimetype,
      format = format,
      title = name,
      description = description
    )
  }
}
