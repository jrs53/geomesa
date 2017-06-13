/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector

import java.io.Closeable

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.NullableBigIntVector
import org.apache.arrow.vector.complex.NullableMapVector
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.locationtech.geomesa.arrow.features.ArrowSimpleFeature
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.EncodingPrecision.EncodingPrecision
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

/**
  * Abstraction for using simple features in Arrow vectors
  *
  * @param sft simple feature type
  * @param underlying underlying arrow vector
  * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
  *                     All values must be provided up front.
  * @param encoding options for encoding
  * @param allocator buffer allocator
  */
class SimpleFeatureVector private (val sft: SimpleFeatureType,
                                   val underlying: NullableMapVector,
                                   val dictionaries: Map[String, ArrowDictionary],
                                   val encoding: SimpleFeatureEncoding)
                                  (implicit allocator: BufferAllocator) extends Closeable {

  private var maxIndex = underlying.getValueCapacity - 1

  // note: writer creates the map child vectors based on the sft, and should be instantiated before the reader
  val writer = new Writer(this)
  val reader = new Reader(this)

  /**
    * double underlying vector capacity
    */
  def expand(): Unit = {
    underlying.reAlloc()
    maxIndex = underlying.getValueCapacity - 1
  }

  /**
    * Clear any simple features currently stored in the vector
    */
  def clear(): Unit = underlying.getMutator.setValueCount(0)

  override def close(): Unit = {
    underlying.close()
    writer.close()
  }

  class Writer(vector: SimpleFeatureVector) {
    private [SimpleFeatureVector] val arrowWriter = vector.underlying.getWriter
    private val idWriter = ArrowAttributeWriter.id(vector.underlying, vector.encoding.fids)
    private [arrow] val attributeWriters = ArrowAttributeWriter(sft, vector.underlying, dictionaries, encoding).toArray

    def set(index: Int, feature: SimpleFeature): Unit = {
      // make sure we have space to write the value
      while (index > vector.maxIndex ) {
        vector.expand()
      }
      arrowWriter.setPosition(index)
      arrowWriter.start()
      idWriter.apply(index, feature.getID)
      var i = 0
      while (i < attributeWriters.length) {
        attributeWriters(i).apply(index, feature.getAttribute(i))
        i += 1
      }
      arrowWriter.end()
    }

    def setValueCount(count: Int): Unit = {
      arrowWriter.setValueCount(count)
      attributeWriters.foreach(_.setValueCount(count))
    }

    private [vector] def close(): Unit = arrowWriter.close()
  }

  class Reader(vector: SimpleFeatureVector) {
    val idReader: ArrowAttributeReader = ArrowAttributeReader.id(vector.underlying, vector.encoding.fids)
    val readers: Array[ArrowAttributeReader] =
      ArrowAttributeReader(sft, vector.underlying, dictionaries, encoding).toArray

    // feature that can be re-populated with calls to 'load'
    val feature: ArrowSimpleFeature = new ArrowSimpleFeature(sft, idReader, readers, -1)

    def get(index: Int): ArrowSimpleFeature = new ArrowSimpleFeature(sft, idReader, readers, index)

    def load(index: Int): Unit = feature.index = index

    def getValueCount: Int = vector.underlying.getAccessor.getValueCount
  }
}

object SimpleFeatureVector {

  val DefaultCapacity = 8096
  val FeatureIdField = "id"
  val DescriptorKey  = "descriptor"

  object EncodingPrecision extends Enumeration {
    type EncodingPrecision = Value
    val Min, Max = Value
  }

  case class SimpleFeatureEncoding(fids: Boolean, geometry: EncodingPrecision, date: EncodingPrecision)

  object SimpleFeatureEncoding {
    private val Min = SimpleFeatureEncoding(fids = false, EncodingPrecision.Min, EncodingPrecision.Min)
    private val Max = SimpleFeatureEncoding(fids = false, EncodingPrecision.Max, EncodingPrecision.Max)
    private val MinWithFids = SimpleFeatureEncoding(fids = true, EncodingPrecision.Min, EncodingPrecision.Min)
    private val MaxWithFids = SimpleFeatureEncoding(fids = true, EncodingPrecision.Max, EncodingPrecision.Max)

    def min(fids: Boolean): SimpleFeatureEncoding = if (fids) { MinWithFids } else { Min }
    def max(fids: Boolean): SimpleFeatureEncoding = if (fids) { MaxWithFids } else { Max }
  }

  /**
    * Create a new simple feature vector
    *
    * @param sft simple feature type
    * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
    *                     All values must be provided up front.
    * @param encoding options for encoding
    * @param capacity initial capacity for number of features able to be stored in vectors
    * @param allocator buffer allocator
    * @return
    */
  def create(sft: SimpleFeatureType,
             dictionaries: Map[String, ArrowDictionary],
             encoding: SimpleFeatureEncoding = SimpleFeatureEncoding.min(false),
             capacity: Int = DefaultCapacity)
            (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    val underlying = NullableMapVector.empty(sft.getTypeName, allocator)
    val vector = new SimpleFeatureVector(sft, underlying, dictionaries, encoding)
    // set capacity after all child vectors have been created by the writers, then allocate
    underlying.setInitialCapacity(capacity)
    underlying.allocateNew()
    vector
  }

  /**
    * Creates a simple feature vector based on an existing arrow vector
    *
    * @param vector arrow vector
    * @param dictionaries map of field names to dictionary values, used for dictionary encoding fields.
    *                     All values must be provided up front.
    * @param allocator buffer allocator
    * @return
    */
  def wrap(vector: NullableMapVector, dictionaries: Map[String, ArrowDictionary])
          (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    import scala.collection.JavaConversions._
    var includeFids = false
    val attributes = ArrayBuffer.empty[String]
    vector.getField.getChildren.foreach { field =>
      if (field.getName == FeatureIdField) {
        includeFids = true
      } else {
        attributes.append(field.getMetadata.get(DescriptorKey))
      }
    }
    val sft = SimpleFeatureTypes.createType(vector.getField.getName, attributes.mkString(","))
    val geomPrecision = {
      val geomVector = Option(sft.getGeometryDescriptor).flatMap(d => Option(vector.getChild(d.getLocalName)))
      val isDouble = geomVector.exists(v => GeometryFields.precisionFromField(v.getField) == FloatingPointPrecision.DOUBLE)
      if (isDouble) { EncodingPrecision.Max } else { EncodingPrecision.Min }
    }
    val datePrecision = {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
      val dateVector = sft.getDtgField.flatMap(d => Option(vector.getChild(d)))
      val isLong = dateVector.exists(_.isInstanceOf[NullableBigIntVector])
      if (isLong) { EncodingPrecision.Max } else { EncodingPrecision.Min }
    }
    val encoding = SimpleFeatureEncoding(includeFids, geomPrecision, datePrecision)
    new SimpleFeatureVector(sft, vector, dictionaries, encoding)
  }

  /**
    * Create a simple feature vector using a new arrow vector
    *
    * @param vector simple feature vector to copy
    * @param underlying arrow vector
    * @param allocator buffer allocator
    * @return
    */
  def clone(vector: SimpleFeatureVector, underlying: NullableMapVector)
           (implicit allocator: BufferAllocator): SimpleFeatureVector = {
    new SimpleFeatureVector(vector.sft, underlying, vector.dictionaries, vector.encoding)
  }
}
