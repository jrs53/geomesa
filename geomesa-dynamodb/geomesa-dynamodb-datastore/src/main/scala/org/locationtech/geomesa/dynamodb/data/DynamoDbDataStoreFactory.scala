/** *********************************************************************
 * Crown Copyright (c) 2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ********************************************************************* */

package org.locationtech.geomesa.dynamodb.data

import java.awt.RenderingHints
import java.io.{IOException, Serializable}
import java.util

import com.amazonaws.SdkClientException
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.dynamodb.data.DynamoDbDataStoreFactory.Params.{CatalogParam, EndpointParam, RegionParam}
import org.locationtech.geomesa.dynamodb.data.DynamoDbDataStoreFactory.{DynamoDbDataStoreConfig, DynamoDbDataStoreQueryConfig}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class DynamoDbDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  // No difference between createNewDataStore and createDataStore
  // Presumably something like file access might differentiate between new & existing but we don't
  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {

    // DynamoDB specific params
    val endpoint = Option[String](EndpointParam.lookup(params))
    val region = RegionParam.lookup(params) // canProcess should have validated

    val client = endpoint match {
      case Some(ep) =>
        logger.info(s"Endpoint specified, connecting to non-cloud DynamoDB at $ep. Region will be ignored.")
        AmazonDynamoDBClientBuilder.standard()
          .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(ep, region))
          .build();

      case None =>
        logger.info(s"Endpoint not specified, connecting to cloud DynamoDB in region $region")
        AmazonDynamoDBClientBuilder.standard()
          .withRegion(region)
          .build();
    }

    // Invoke operation to test connection
    try {
      client.listTables()
    }
    catch {
      case ex: SdkClientException =>
        logger.warn("Unable to use DynamoDB connection, data store creation failed", ex)
        return null
    }

    // Generic GeoMesa Datastore params (to pass to datastore parent)
    val audit = if (AuditQueriesParam.lookup(params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "dynamodb")
    } else {
      None
    }
    val generateStats = GenerateStatsParam.lookup(params)
    val namespace = Option(NamespaceParam.lookUp(params).asInstanceOf[String])
    val catalog = CatalogParam.lookup(params)

    // Generic GeoMesa Datastore query params (to pass to datastore parent)
    val queryConfig = DynamoDbDataStoreQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params),
      caching = CachingParam.lookup(params)
    )

    new DynamoDbDataStore(client,
      DynamoDbDataStoreConfig(audit, generateStats, queryConfig, namespace, catalog)
    )
  }

  // Libraries all ought to be in place by Maven packaging process
  override def isAvailable = true

  override def getDisplayName: String = DynamoDbDataStoreFactory.DisplayName

  override def getDescription: String = DynamoDbDataStoreFactory.Description

  // TODO: need to add any more like the other data stores?
  override def getParametersInfo: Array[Param] = DynamoDbDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def canProcess(params: util.Map[String, Serializable]): Boolean = DynamoDbDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object DynamoDbDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  override val DisplayName = "DynamoDB (GeoMesa)"
  override val Description = "Amazon Web Service DynamoDB key-value & document database"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      EndpointParam,
      RegionParam,
      CatalogParam
    )

  // Note authentication parameters are handled per https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html
  object Params extends GeoMesaDataStoreParams {

    val EndpointParam =
      new GeoMesaParam[String](
        "dynamodb.endpoint",
        "DynamoDB endpoint; only used with local instances",
        optional = true,
        supportsNiFiExpressions = true)

    val RegionParam =
      new GeoMesaParam[String](
        "dynamodb.region",
        "DynamoDB region",
        optional = false,
        supportsNiFiExpressions = true)

    val CatalogParam =
      new GeoMesaParam[String](
        "dynamodb.catalog",
        "Name of GeoMesa catalog table",
        optional = false,
        supportsNiFiExpressions = true)
  }

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    // Region must be specified and in list of valid regions
    val hasRegion = RegionParam.exists(params)

    try {
      val region = Regions.fromName(RegionParam.lookup(params))
      // If didn't throw exception, all okay
      true
    }
    catch {
      case e: IOException =>
        logger.warn("Region parameter not found or wrong type")
        false
      case e: IllegalArgumentException =>
        logger.warn(s"Region parameter ${RegionParam.lookup(params)} is not a recognised region")
        false
    }
  }


  case class DynamoDbDataStoreConfig(
                                      // From GeoMesaDataStoreConfig
                                      audit: Option[(AuditWriter, AuditProvider, String)],
                                      generateStats: Boolean,
                                      queries: DynamoDbDataStoreQueryConfig,
                                      // From NamespaceConfig
                                      namespace: Option[String],
                                      catalog: String
                                    ) extends GeoMesaDataStoreConfig

  case class DynamoDbDataStoreQueryConfig(
                                           // From DataStoreQueryConfig
                                           threads: Int,
                                           timeout: Option[Long],
                                           looseBBox: Boolean,
                                           caching: Boolean
                                         ) extends DataStoreQueryConfig


}
