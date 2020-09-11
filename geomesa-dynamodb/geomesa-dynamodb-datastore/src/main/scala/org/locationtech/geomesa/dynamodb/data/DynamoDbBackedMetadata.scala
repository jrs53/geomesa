package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, PrimaryKey, TableWriteItems}
import com.amazonaws.services.dynamodbv2.model._
import org.locationtech.geomesa.index.metadata
import org.locationtech.geomesa.index.metadata.{KeyValueStoreMetadata, MetadataSerializer, TableBasedMetadata}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.collection.JavaConverters._


class DynamoDbBackedMetadata[T](private val client: AmazonDynamoDB,
                                private val catalog: String,
                                private val metadataSerializer: MetadataSerializer[T])
  extends KeyValueStoreMetadata[T] {

  // Using the document API
  private val dynamoDb = new DynamoDB(client)

  // Use the name of the catalog as the metadata table name
  val metadataTableName: String = catalog

  // Name of the primary key "column"
  val primaryKeyName = "key"

  // Name of the value "column"
  val valueName = "value"

  // Proactively create table (if needed)
  if (!dynamoDb.listTables().asScala.map(_.getTableName).toList.contains(metadataTableName)) {

    logger.info("Creating new metadata table")

    // Create table with just a simple primary key
    // TODO: add ProvisionedThroughput options
    dynamoDb.createTable(
      new CreateTableRequest(
        metadataTableName,
        Seq(new KeySchemaElement(primaryKeyName, KeyType.HASH)).asJava // Simple primary key
      ).withAttributeDefinitions(
        new AttributeDefinition(primaryKeyName, ScalarAttributeType.B)
      ).withBillingMode(
        BillingMode.PAY_PER_REQUEST
      )
    ).waitForActive()

  } else {
    logger.info("Using existing metadata table")
  }

  /**
   * Writes row/value pairs
   *
   * @param rows row/values
   */
  override protected def write(rows: Seq[(Array[Byte], Array[Byte])]): Unit = {
    logger.info(s"Writing ${rows.size} row(s) to $metadataTableName")

    // Create batches of items to write. Max 25 items per batch write.
    // TODO: handle exceeding individual item size, or overall batch size
    val groupedItems = rows.map(row =>
      new Item()
        .withPrimaryKey(primaryKeyName, row._1)
        .withBinary(valueName, row._2)
    ).grouped(25)

    // Write each batch
    groupedItems.foreach { items =>
      val twi = new TableWriteItems(metadataTableName).withItemsToPut(items.asJava)

      // Try to write batch of items
      logger.info(s"Writing ${items.size} row(s) in batch to $metadataTableName")
      var outcome = dynamoDb.batchWriteItem(twi)

      // Keep retrying until all written
      do {
        // Check for unprocessed item which could happen if exceed provisioned throughput
        if (outcome.getUnprocessedItems.size == 0) {
          logger.debug("All items written")
        }
        else {
          logger.debug(s"${outcome.getUnprocessedItems.size} unprocessed items found, retrying after delay")
          Thread.sleep(1000) // TODO: exponential backoff
          outcome = dynamoDb.batchWriteItemUnprocessed(outcome.getUnprocessedItems)
        }
      }
      while (outcome.getUnprocessedItems.size > 0)
    }

  }

  /**
   * Deletes multiple rows
   *
   * @param rows rows
   */
  override protected def delete(rows: Seq[Array[Byte]]): Unit = {
    logger.info(s"Deleting ${rows.size} row(s) from $metadataTableName")

    // Delete each row in turn
    rows.foreach { row =>
      dynamoDb.getTable(metadataTableName).deleteItem(new PrimaryKey(primaryKeyName, row))
      // TODO: need to check outcome?
    }

  }

  /**
   * Reads a value from the underlying table
   *
   * @param row row
   * @return value, if it exists
   */
  override protected def scanValue(row: Array[Byte]): Option[Array[Byte]] = {
    logger.info(s"Scanning $metadataTableName for ${new String(row, "UTF8")}")

    Option(dynamoDb.getTable(metadataTableName).getItem(new PrimaryKey(primaryKeyName, row))) match {
      case Some(item) => Some(item.getBinary(valueName))
      case None => None
    }
  }

  /**
   * Reads row keys from the underlying table
   *
   * @param prefix row key prefix
   * @return matching row keys and values
   */
  override protected def scanRows(prefix: Option[Array[Byte]]): CloseableIterator[(Array[Byte], Array[Byte])] = {
    logger.info(s"Scanning $metadataTableName for prefix ${new String(prefix.getOrElse(Array()), "UTF-8")}")

    // Don't expect table to be too big, so fetch all rows and then filter here
    val allRows = dynamoDb.getTable(metadataTableName).scan().asScala.iterator.map(
      // Convert to (key, value) pairs
      item => (item.getBinary(primaryKeyName), item.getBinary(valueName))
    )

    // Apply prefix filter, if specified
    prefix match {
      case None => CloseableIterator(allRows)
      case Some(p) => CloseableIterator(allRows.filter({ case (k, _) => k.startsWith(p) }))
    }

  }

  /**
   * Serializer
   *
   * @return
   */
  override def serializer: metadata.MetadataSerializer[T] = metadataSerializer

  /**
   * Checks if the underlying table exists
   *
   * @return
   */
  override protected def checkIfTableExists: Boolean = true // table has been proactively created

  /**
   * Creates the underlying table
   */
  override protected def createTable(): Unit = {} // no need, table already created

  /**
   * Create an instance to use for backup
   *
   * @param timestamp formatted timestamp for the current time
   * @return
   */
  override protected def createEmptyBackup(timestamp: String): TableBasedMetadata[T] = {
    logger.info(s"Creating backup timestamp $timestamp")
    new DynamoDbBackedMetadata(client, s"${metadataTableName}_${timestamp}_bak", serializer)
  }

  override def close(): Unit = {} // TODO: need to do anything here?

}
