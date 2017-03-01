/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, Scan}
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore, HBaseDataStoreFactory}
import org.locationtech.geomesa.jobs.GeoMesaConfigurator
import org.locationtech.geomesa.spark.SpatialRDDProvider
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.simple.SimpleFeature

class HBaseSpatialRDDProvider extends SpatialRDDProvider {
  import org.locationtech.geomesa.spark.CaseInsensitiveMapFix._

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    HBaseDataStoreFactory.canProcess(params)

  def rdd(conf: Configuration,
          sc: SparkContext,
          dsParams: Map[String, String],
          origQuery: Query): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[HBaseDataStore]

    // get the query plan to set up the iterators, ranges, etc
    lazy val sft = ds.getSchema(origQuery.getTypeName)
    lazy val qp = ds.getQueryPlan(origQuery).head

    if (ds == null || sft == null || qp.isInstanceOf[EmptyPlan]) {
      sc.emptyRDD[SimpleFeature]
    } else {
      import org.locationtech.geomesa.index.conf.QueryHints._

      val query = ds.queryPlanner.configureQuery(origQuery, sft)
      GeoMesaConfigurator.setSerialization(conf)
      query.getHints.getTransformSchema.foreach(GeoMesaConfigurator.setTransformSchema(conf, _))
      GeoMesaConfigurator.setTable(conf, qp.table.getNameAsString)
      GeoMesaConfigurator.setDataStoreInParams(conf, dsParams)
      GeoMesaConfigurator.setFeatureType(conf, sft.getTypeName)
      qp.filter.secondary.foreach { f => GeoMesaConfigurator.setFilter(conf, ECQL.toCQL(f)) }
      val scans = qp.ranges.map { s =>
        val scan = s
        // need to set the table name in each scan
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, qp.table.getName)
        convertScanToString(scan)
      }
      conf.setStrings(MultiTableInputFormat.SCANS, scans: _*)

      sc.newAPIHadoopRDD(conf, classOf[GeoMesaHBaseInputFormat], classOf[Text], classOf[SimpleFeature]).map(U => U._2)
    }
  }

  private def convertScanToString(scan: org.apache.hadoop.hbase.client.Query): String = scan match {
    case g: Get =>
      val proto = ProtobufUtil.toGet(g)
      Base64.encodeBytes(proto.toByteArray)

    case s: Scan =>
      val proto = ProtobufUtil.toScan(s)
      Base64.encodeBytes(proto.toByteArray)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[HBaseDataStore]
    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call createSchema on the DataStore first.")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams).asInstanceOf[HBaseDataStore]
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      try {
        iter.foreach { rawFeature =>
          FeatureUtils.copyToWriter(featureWriter, rawFeature, overrideFid = true)
          featureWriter.write()
        }
      } finally {
        CloseQuietly(featureWriter)
        ds.dispose()
      }
    }
  }

}
