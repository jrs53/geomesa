/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.nio.charset.StandardCharsets
import java.util.Date

import com.vividsolutions.jts.geom._
import org.geotools.data.FeatureReader
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.{AttributeTypeBuilder, FeatureIterator}
import org.geotools.geometry.DirectPosition2D
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.joda.time.DateTime
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{TimePeriod, XZSFC}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.index.VisibilityLevel.VisibilityLevel
import org.locationtech.geomesa.utils.stats.Cardinality._
import org.locationtech.geomesa.utils.stats.IndexCoverage._
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.temporal.Instant

import scala.reflect.ClassTag
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

object Conversions {

  @deprecated("use CloseableIterator")
  class RichSimpleFeatureIterator(iter: FeatureIterator[SimpleFeature]) extends SimpleFeatureIterator
      with Iterator[SimpleFeature] {
    private[this] var open = true

    def isClosed = !open

    def hasNext = {
      if (isClosed) false
      if(iter.hasNext) true else{close(); false}
    }
    def next() = iter.next
    def close() { if(!isClosed) {iter.close(); open = false} }
  }

  @deprecated("use CloseableIterator")
  implicit class RichSimpleFeatureReader(val r: FeatureReader[SimpleFeatureType, SimpleFeature]) extends AnyVal {
    def toIterator: Iterator[SimpleFeature] = new Iterator[SimpleFeature] {
      override def hasNext: Boolean = r.hasNext
      override def next(): SimpleFeature = r.next()
    }
  }

  @deprecated("use CloseableIterator")
  implicit def toRichSimpleFeatureIterator(iter: SimpleFeatureIterator): RichSimpleFeatureIterator = new RichSimpleFeatureIterator(iter)
  @deprecated("use CloseableIterator")
  implicit def toRichSimpleFeatureIteratorFromFI(iter: FeatureIterator[SimpleFeature]): RichSimpleFeatureIterator = new RichSimpleFeatureIterator(iter)

  implicit def opengisInstantToJodaInstant(instant: Instant): org.joda.time.Instant = new DateTime(instant.getPosition.getDate).toInstant
  implicit def jodaInstantToOpengisInstant(instant: org.joda.time.Instant): org.opengis.temporal.Instant = new DefaultInstant(new DefaultPosition(instant.toDate))
  implicit def jodaIntervalToOpengisPeriod(interval: org.joda.time.Interval): org.opengis.temporal.Period =
    new DefaultPeriod(interval.getStart.toInstant, interval.getEnd.toInstant)


  implicit class RichCoord(val c: Coordinate) extends AnyVal {
    def toPoint2D = new DirectPosition2D(c.x, c.y)
  }

  implicit class RichGeometry(val geom: Geometry) extends AnyVal {
    def bufferMeters(meters: Double): Geometry = geom.buffer(distanceDegrees(meters))
    def distanceDegrees(meters: Double): Double = GeometryUtils.distanceDegrees(geom, meters)
    def safeCentroid(): Point = {
      val centroid = geom.getCentroid
      if (java.lang.Double.isNaN(centroid.getCoordinate.x) || java.lang.Double.isNaN(centroid.getCoordinate.y)) {
        geom.getEnvelope.getCentroid
      } else {
        centroid
      }
    }
  }

  implicit class RichSimpleFeature(val sf: SimpleFeature) extends AnyVal {
    def geometry: Geometry = sf.getDefaultGeometry.asInstanceOf[Geometry]
    def polygon: Polygon = sf.getDefaultGeometry.asInstanceOf[Polygon]
    def point: Point = sf.getDefaultGeometry.asInstanceOf[Point]
    def lineString: LineString = sf.getDefaultGeometry.asInstanceOf[LineString]
    def multiPolygon: MultiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
    def multiPoint: MultiPoint = sf.getDefaultGeometry.asInstanceOf[MultiPoint]
    def multiLineString: MultiLineString = sf.getDefaultGeometry.asInstanceOf[MultiLineString]

    def get[T](i: Int): T = sf.getAttribute(i).asInstanceOf[T]
    def get[T](name: String): T = sf.getAttribute(name).asInstanceOf[T]

    def getDouble(str: String): Double = {
      val ret = sf.getAttribute(str)
      ret match {
        case d: java.lang.Double  => d
        case f: java.lang.Float   => f.toDouble
        case i: java.lang.Integer => i.toDouble
        case _                    => throw new Exception(s"Input $ret is not a numeric type.")
      }
    }

    def userData[T](key: AnyRef)(implicit ct: ClassTag[T]): Option[T] = {
      Option(sf.getUserData.get(key)).flatMap {
        case ct(x) => Some(x)
        case _ => None
      }
    }
  }
}

object RichIterator {
  implicit class RichIterator[T](val iter: Iterator[T]) extends AnyVal {
    def head = iter.next()
    def headOption = if (iter.hasNext) Some(iter.next()) else None
  }
}

/**
 * Contains GeoMesa specific attribute descriptor information
 */
object RichAttributeDescriptors {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeConfigs._
  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions._

  // noinspection AccessorLikeMethodIsEmptyParen
  implicit class RichAttributeDescriptor(val ad: AttributeDescriptor) extends AnyVal {

    def setIndexCoverage(coverage: IndexCoverage): Unit = ad.getUserData.put(OPT_INDEX, coverage.toString)

    def getIndexCoverage(): IndexCoverage = {
      val coverage = ad.getUserData.get(OPT_INDEX).asInstanceOf[String]
      if (coverage == null) { IndexCoverage.NONE } else {
        Try(IndexCoverage.withName(coverage)).getOrElse {
          if (java.lang.Boolean.valueOf(coverage)) {
            IndexCoverage.JOIN
          } else {
            IndexCoverage.NONE
          }
        }
      }
    }

    def setKeepStats(enabled: Boolean): Unit = if (enabled) {
      ad.getUserData.put(OPT_STATS, "true")
    } else {
      ad.getUserData.remove(OPT_STATS)
    }
    def isKeepStats(): Boolean = Option(ad.getUserData.get(OPT_STATS)).exists(_ == "true")

    def isIndexValue(): Boolean = Option(ad.getUserData.get(OPT_INDEX_VALUE)).exists(_ == "true")

    def setCardinality(cardinality: Cardinality): Unit =
      ad.getUserData.put(OPT_CARDINALITY, cardinality.toString)

    def getCardinality(): Cardinality =
      Option(ad.getUserData.get(OPT_CARDINALITY).asInstanceOf[String])
          .flatMap(c => Try(Cardinality.withName(c)).toOption).getOrElse(Cardinality.UNKNOWN)

    def isJson(): Boolean = Option(ad.getUserData.get(OPT_JSON)).exists(_ == "true")

    def setBinTrackId(opt: Boolean): Unit = ad.getUserData.put(OPT_BIN_TRACK_ID, opt.toString)

    def isBinTrackId(): Boolean = Option(ad.getUserData.get(OPT_BIN_TRACK_ID)).exists(_ == "true")

    def setListType(typ: Class[_]): Unit = ad.getUserData.put(USER_DATA_LIST_TYPE, typ.getName)

    def getListType(): Class[_] = tryClass(ad.getUserData.get(USER_DATA_LIST_TYPE).asInstanceOf[String])

    def setMapTypes(keyType: Class[_], valueType: Class[_]): Unit = {
      ad.getUserData.put(USER_DATA_MAP_KEY_TYPE, keyType.getName)
      ad.getUserData.put(USER_DATA_MAP_VALUE_TYPE, valueType.getName)
    }

    def getMapTypes(): (Class[_], Class[_]) =
      (tryClass(ad.getUserData.get(USER_DATA_MAP_KEY_TYPE)), tryClass(ad.getUserData.get(USER_DATA_MAP_VALUE_TYPE)))

    private def tryClass(value: AnyRef): Class[_] = Try(Class.forName(value.asInstanceOf[String])).getOrElse(null)

    def isIndexed: Boolean = getIndexCoverage() match {
      case IndexCoverage.FULL | IndexCoverage.JOIN => true
      case IndexCoverage.NONE => false
    }

    def isList: Boolean = ad.getUserData.containsKey(USER_DATA_LIST_TYPE)

    def isMap: Boolean =
      ad.getUserData.containsKey(USER_DATA_MAP_KEY_TYPE) && ad.getUserData.containsKey(USER_DATA_MAP_VALUE_TYPE)

    def isMultiValued: Boolean = isList || isMap
  }

  implicit class RichAttributeTypeBuilder(val builder: AttributeTypeBuilder) extends AnyVal {

    def indexCoverage(coverage: IndexCoverage) = builder.userData(OPT_INDEX, coverage.toString)

    def indexValue(indexValue: Boolean) = builder.userData(OPT_INDEX_VALUE, indexValue)

    def cardinality(cardinality: Cardinality) = builder.userData(OPT_CARDINALITY, cardinality.toString)

    def collectionType(typ: Class[_]) = builder.userData(USER_DATA_LIST_TYPE, typ)

    def mapTypes(keyType: Class[_], valueType: Class[_]) =
      builder.userData(USER_DATA_MAP_KEY_TYPE, keyType).userData(USER_DATA_MAP_VALUE_TYPE, valueType)
  }
}

object RichSimpleFeatureType {

  import RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConversions._

  // in general we store everything as strings so that it's easy to pass to accumulo iterators
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    import SimpleFeatureTypes.Configs._
    import SimpleFeatureTypes.InternalConfigs._

    def getGeomField: String = {
      val gd = sft.getGeometryDescriptor
      if (gd == null) null else gd.getLocalName
    }
    def getGeomIndex: Int = sft.indexOf(getGeomField)

    def getDtgField: Option[String] = userData[String](DEFAULT_DATE_KEY)
    def getDtgIndex: Option[Int] = getDtgField.map(sft.indexOf).filter(_ != -1)
    def getDtgDescriptor = getDtgIndex.map(sft.getDescriptor)
    def clearDtgField(): Unit = sft.getUserData.remove(DEFAULT_DATE_KEY)
    def setDtgField(dtg: String): Unit = {
      val descriptor = sft.getDescriptor(dtg)
      require(descriptor != null && classOf[Date].isAssignableFrom(descriptor.getType.getBinding),
        s"Invalid date field '$dtg' for schema $sft")
      sft.getUserData.put(DEFAULT_DATE_KEY, dtg)
    }

    def getStIndexSchema: String = userData[String](ST_INDEX_SCHEMA_KEY).orNull
    def setStIndexSchema(schema: String): Unit = sft.getUserData.put(ST_INDEX_SCHEMA_KEY, schema)

    def getBinTrackId: Option[String] = sft.getAttributeDescriptors.find(_.isBinTrackId()).map(_.getLocalName)

    def getSchemaVersion: Int =
      userData[String](SCHEMA_VERSION_KEY).map(_.toInt).getOrElse(CURRENT_SCHEMA_VERSION)
    def setSchemaVersion(version: Int): Unit = sft.getUserData.put(SCHEMA_VERSION_KEY, version.toString)

    def isPoints = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding == classOf[Point]
    }
    def nonPoints = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding != classOf[Point]
    }
    def isLines = {
      val gd = sft.getGeometryDescriptor
      gd != null && gd.getType.getBinding == classOf[LineString]
    }

    def getVisibilityLevel: VisibilityLevel = userData[String](VIS_LEVEL_KEY) match {
      case None        => VisibilityLevel.Feature
      case Some(level) => VisibilityLevel.withName(level.toLowerCase)
    }
    def setVisibilityLevel(vis: VisibilityLevel): Unit = sft.getUserData.put(VIS_LEVEL_KEY, vis.toString)

    def getZ3Interval: TimePeriod = userData[String](Z3_INTERVAL_KEY) match {
      case None    => TimePeriod.Week
      case Some(i) => TimePeriod.withName(i.toLowerCase)
    }
    def setZ3Interval(i: TimePeriod): Unit = sft.getUserData.put(Z3_INTERVAL_KEY, i.toString)

    def getXZPrecision: Short = userData[String](XZ_PRECISION_KEY).map(_.toShort).getOrElse(XZSFC.DefaultPrecision)
    def setXZPrecision(p: Short): Unit = sft.getUserData.put(XZ_PRECISION_KEY, p.toString)

    //  If no user data is specified when creating a new SFT, we should default to 'true'.
    def isTableSharing: Boolean = userData[String](TABLE_SHARING_KEY).forall(_.toBoolean)
    def setTableSharing(sharing: Boolean): Unit = sft.getUserData.put(TABLE_SHARING_KEY, sharing.toString)

    def getTableSharingPrefix: String = userData[String](SHARING_PREFIX_KEY).getOrElse("")
    def setTableSharingPrefix(prefix: String): Unit = sft.getUserData.put(SHARING_PREFIX_KEY, prefix)

    def getTableSharingBytes: Array[Byte] = if (sft.isTableSharing) {
      sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
    } else {
      Array.empty[Byte]
    }

    // gets (name, version, mode) of enabled indices
    def getIndices: Seq[(String, Int, IndexMode)] = {
      def toTuple(string: String): (String, Int, IndexMode) = {
        val Array(n, v, m) = string.split(":")
        (n, v.toInt, new IndexMode(m.toInt))
      }
      userData[String](INDEX_VERSIONS).map(_.split(",").map(toTuple).toSeq).getOrElse(List.empty)
    }
    def setIndices(indices: Seq[(String, Int, IndexMode)]): Unit =
      sft.getUserData.put(INDEX_VERSIONS, indices.map { case (n, v, m) => s"$n:$v:${m.flag}"}.mkString(","))

    def setUserDataPrefixes(prefixes: Seq[String]): Unit = sft.getUserData.put(USER_DATA_PREFIX, prefixes.mkString(","))
    def getUserDataPrefixes: Seq[String] =
      Seq(GEOMESA_PREFIX) ++ userData[String](USER_DATA_PREFIX).map(_.split(",")).getOrElse(Array.empty)

    def getTableSplitter: Option[Class[_]] = userData[String](TABLE_SPLITTER).map(Class.forName)
    def getTableSplitterOptions: Map[String, String] =
      userData[String](TABLE_SPLITTER_OPTS).map(new KVPairParser().parse).getOrElse(Map.empty)

    def setZShards(splits: Int): Unit = sft.getUserData.put(Z_SPLITS_KEY, splits.toString)
    def getZShards: Int = userData[String](Z_SPLITS_KEY).map(_.toInt).getOrElse(4)

    def setAttributeShards(splits: Int): Unit = sft.getUserData.put(ATTR_SPLITS_KEY, splits.toString)
    def getAttributeShards: Int = userData[String](ATTR_SPLITS_KEY).map(_.toInt).getOrElse(4)

    def userData[T](key: AnyRef): Option[T] = Option(sft.getUserData.get(key).asInstanceOf[T])

    def getKeywords: Set[String] = userData[String](KEYWORDS_KEY).getOrElse("").split(KEYWORDS_DELIMITER).toSet

    def addKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(KEYWORDS_KEY, getKeywords.union(keywords).mkString(KEYWORDS_DELIMITER))

    def removeKeywords(keywords: Set[String]): Unit =
      sft.getUserData.put(KEYWORDS_KEY, getKeywords.diff(keywords).mkString(KEYWORDS_DELIMITER))

    def removeAllKeywords(): Unit = sft.getUserData.remove(KEYWORDS_KEY)
  }

  private class KVPairParser(pairSep: String = ",", kvSep: String = ":") extends JavaTokenParsers {
    def key = "[0-9a-zA-Z\\.]+".r
    def value = s"[^($pairSep)^($kvSep)]+".r

    def keyValue = key ~ kvSep ~ value ^^ { case key ~ sep ~ value => key -> value }
    def keyValueList = repsep(keyValue, pairSep) ^^ { x => x.toMap }

    def parse(s: String): Map[String, String] = parse(keyValueList, s.trim) match {
      case Success(result, next) if next.atEnd => result
      case NoSuccess(msg, next) if next.atEnd => throw new IllegalArgumentException(s"Error parsing spec '$s' : $msg")
      case other => throw new IllegalArgumentException(s"Error parsing spec '$s' : $other")
    }
  }
}
