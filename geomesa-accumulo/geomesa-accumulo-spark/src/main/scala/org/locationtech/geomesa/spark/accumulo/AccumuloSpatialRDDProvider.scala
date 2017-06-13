/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.accumulo

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapred.AbstractInputFormat
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator
import org.apache.accumulo.core.client.security.tokens.{KerberosToken, PasswordToken}
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.util.{Pair => AccPair}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{NewHadoopRDD, RDD}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.accumulo.index.{AccumuloQueryPlan, EmptyPlan}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.jobs.accumulo.AccumuloJobUtils
import org.locationtech.geomesa.jobs.mapreduce._
import org.locationtech.geomesa.spark.{SpatialRDD, SpatialRDDProvider}
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

class AccumuloSpatialRDDProvider extends SpatialRDDProvider with LazyLogging {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def rdd(conf: Configuration,
                   sc: SparkContext,
                   params: Map[String, String],
                   query: Query): SpatialRDD = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    lazy val transform = query.getHints.getTransformSchema

    def queryPlanToRDD(sft: SimpleFeatureType, qp: AccumuloQueryPlan, conf: Configuration) = {
      if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
        sc.emptyRDD[SimpleFeature]
      } else {
        InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, qp.table)
        InputConfigurator.setRanges(classOf[AccumuloInputFormat], conf, qp.ranges)
        qp.iterators.foreach(InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, _))

        if (qp.columnFamilies.nonEmpty) {
          val cf = qp.columnFamilies.map(cf => new AccPair[Text, Text](cf, null))
          InputConfigurator.fetchColumns(classOf[AccumuloInputFormat], conf, cf)
        }

        InputConfigurator.setBatchScan(classOf[AccumuloInputFormat], conf, true)
        InputConfigurator.setBatchScan(classOf[GeoMesaAccumuloInputFormat], conf, true)
        GeoMesaConfigurator.setSerialization(conf)
        GeoMesaConfigurator.setTable(conf, qp.table)
        GeoMesaConfigurator.setDataStoreInParams(conf, params)
        GeoMesaConfigurator.setFeatureType(conf, sft.getTypeName)

        // set the secondary filter if it exists and is  not Filter.INCLUDE
        qp.filter.secondary
          .collect { case f if f != Filter.INCLUDE => f }
          .foreach { f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)) }

        transform.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))

        // Configure Auths from DS
        val auths = Option(AccumuloDataStoreParams.authsParam.lookUp(params).asInstanceOf[String])
        auths.foreach { a =>
          val authorizations = new Authorizations(a.split(","): _*)
          InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, authorizations)
        }

        // We soon want to call this
        // sc.newAPIHadoopRDD(conf, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
        // But we need access to the JobConf that this creates internally and doesn't expose, so we repeat (most of) the code here

        // From sc.newAPIHadoopRDD, but can't implement this here
        // assertNotStopped()

        // From sc.newAPIHadoopRDD
        // Add necessary security credentials to the JobConf. Required to access secure HDFS.
        val jconf = new JobConf(conf)
        SparkHadoopUtil.get.addCredentials(jconf)

        // Get username from params
        val username = AccumuloDataStoreParams.userParam.lookUp(params).toString

        // Get password or keytabPath from params. Precisely one of these should be set due to prior validation
        val password = AccumuloDataStoreParams.passwordParam.lookUp(params)
        val keytabPath = AccumuloDataStoreParams.keytabPathParam.lookUp(params)

        // Create authentication token according to password or Kerberos
        val authToken = if (password != null) {
          new PasswordToken(password.toString.getBytes)
        } else {
          // setConnectorInfo will take care of creating a DelegationToken for us
          new KerberosToken(username, new java.io.File(keytabPath.toString), true)
        }

        // Get params and set ZooKeeperInstance
        val instance = AccumuloDataStoreParams.instanceIdParam.lookUp(params).asInstanceOf[String]
        val zookeepers = AccumuloDataStoreParams.zookeepersParam.lookUp(params).asInstanceOf[String]
        AbstractInputFormat.setZooKeeperInstance(jconf, new ClientConfiguration()
          .withInstance(instance).withZkHosts(zookeepers).withSasl(authToken.isInstanceOf[KerberosToken]))

        // Set connectorInfo. This will add a DelegationToken to jconf.getCredentials
        val user = AccumuloDataStoreParams.userParam.lookUp(params).asInstanceOf[String]
        AbstractInputFormat.setConnectorInfo(jconf, user, authToken)

        // Iterate over tokens in credentials and add the Accumulo one to the configuration directly
        // This is because the credentials seem to disappear between here and the YARN executor
        // See https://stackoverflow.com/questions/44525351/delegation-tokens-with-accumulo-spark
        for (tok <- jconf.getCredentials.getAllTokens) {
          if (tok.getKind.toString=="ACCUMULO_AUTH_TOKEN") {
            logger.info("Adding ACCUMULO_AUTH_TOKEN to configuration")
            jconf.set("org.locationtech.geomesa.token", tok.encodeToUrlString())
          }
        }

        // From sc.newAPIHadoopRDD
        new NewHadoopRDD(sc, classOf[GeoMesaAccumuloInputFormat], classOf[Text], classOf[SimpleFeature], jconf).map(U => U._2)
      }
    }

    try {
      // get the query plan to set up the iterators, ranges, etc
      // getMultipleQueryPlan will return the fallback if any
      // element of the plan is a JoinPlan
      val sft = ds.getSchema(query.getTypeName)
      val qps = AccumuloJobUtils.getMultipleQueryPlan(ds, query)

      // can return a union of the RDDs because the query planner *should*
      // be rewriting ORs to make them logically disjoint
      // e.g. "A OR B OR C" -> "A OR (B NOT A) OR ((C NOT A) NOT B)"
      val sfrdd = if (qps.length == 1)
          queryPlanToRDD(sft, qps.head, conf) // no union needed for single query plan
        else
          sc.union(qps.map(queryPlanToRDD(sft, _, new Configuration(conf))))
      SpatialRDD(sfrdd, transform.getOrElse(sft))
    } finally {
      if (ds != null) {
        ds.dispose()
      }
    }
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param params
    * @param typeName
    */
  def save(rdd: RDD[SimpleFeature], params: Map[String, String], typeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    try {
      require(ds.getSchema(typeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
      val featureWriter = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, overrideFid = true)
          featureWriter.write()
        }
      } finally {
        IOUtils.closeQuietly(featureWriter)
        ds.dispose()
      }
    }
  }

}
