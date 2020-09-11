/** *********************************************************************
 * Crown Copyright (c) 2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ********************************************************************* */

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import org.junit.runner.RunWith
import org.locationtech.geomesa.index.metadata.MetadataStringSerializer
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class DynamoDbBackedMetadataTest extends Specification {

  // TODO: consider port collisions
  val DYNAMODB_PORT = "8000"

  // TODO: neat way to either disable running tests in parallel, or safely share single Local DynamoDB

  // DynamoDB Local needs native sqlite4java libs; maven-dependency-plugin should put them here
  System.setProperty("sqlite4java.library.path", "target/test-classes/native-libs")

  // Create local DynamoDB for tests. Using -sharedDb so can inspect with NoSQL Workbench
  private val server = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory", "-port", DYNAMODB_PORT, "-sharedDb"))
  server.start()

  private val client = AmazonDynamoDBClientBuilder.standard()
    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
      s"http://localhost:$DYNAMODB_PORT",
      "eu-west-2")
    ).build()

  // Will use the document API
  val dynamoDb = new DynamoDB(client)

  // This will proactively create tables
  val dynamoDbBackedMetadata = new DynamoDbBackedMetadata[String](client, "test_catalog", MetadataStringSerializer)

  // Sample data to insert
  private val kvPairs = (1 to 450).map(n => (s"key $n", s"value $n")).toMap

  sequential

  "DynamoDbBackedMetadata" should {

    "have created metadata table" in {

      // Get all table names from the server
      val tableNames = dynamoDb.listTables().asScala.toList.map(_.getTableName)

      // Metadata table must have been created
      tableNames must contain(dynamoDbBackedMetadata.metadataTableName)

    }

    // Ideally would test methods in DynamoDbBackedMetadata directly, but can't since they are protected
    // Hence test via public API from TableBasedMetadata

    // ensureTableExists
    "ensure table exists" in {
      dynamoDbBackedMetadata.ensureTableExists()
      success // if no exceptions thrown
    }

    // insert
    "insert single metadata item" in {
      dynamoDbBackedMetadata.insert("test_type", "my_test_key", "my_test_value")
      val rowCount = dynamoDb.getTable(dynamoDbBackedMetadata.metadataTableName).scan().iterator().asScala.size
      rowCount must equalTo(1)
    }

    // insert (multiple items)
    "insert multiple metadata items" in {

      // Insert sample data
      dynamoDbBackedMetadata.insert("test_type", kvPairs)

      // Check size, remembering previous test has taken place
      val rowCount = dynamoDb.getTable(dynamoDbBackedMetadata.metadataTableName).scan().iterator().asScala.size
      rowCount must equalTo(kvPairs.size + 1)
    }

    // getFeatureTypes
    // Can't test without creating schema?

    // invalidateCache
    // Can't test directly, but doesn't touch DynamoDB?

    // read (present)
    "read single item by key" in {
      val value = dynamoDbBackedMetadata.read("test_type", "my_test_key", cache = false)
      value must beSome("my_test_value")
    }

    // read (absent)
    "read missing item by key" in {
      val value = dynamoDbBackedMetadata.read("test_type", "missing_key", cache = false)
      value must beNone
    }

    // scan (with prefix)
    "scan items" in {
      // Should match all items apart from the single one we inserted earlier (with key "my_test_key")
      val items = dynamoDbBackedMetadata.scan("test_type", "key ", cache = false)
      items.size must beEqualTo(kvPairs.size)
    }

    // remove (single)
    "remove single item" in {

      // Remove the single item we inserted
      dynamoDbBackedMetadata.remove("test_type", "my_test_key")

      // Check size
      val rowCount = dynamoDb.getTable(dynamoDbBackedMetadata.metadataTableName).scan().iterator().asScala.size
      rowCount must equalTo(kvPairs.size)
    }

    // remove (multiple)
    "remove multiple items" in {

      val keysToRemove = Seq("key 1", "key 2", "key 3")

      // Remove the specified items
      dynamoDbBackedMetadata.remove("test_type", keysToRemove)

      // Check size
      val rowCount = dynamoDb.getTable(dynamoDbBackedMetadata.metadataTableName).scan().iterator().asScala.size
      rowCount must equalTo(kvPairs.size - keysToRemove.size)
    }

    // delete
    "delete a type" in {

      // Add a single item with a different type name that shouldn't be deleted
      dynamoDbBackedMetadata.insert("not_to_be_deleted", "do_not_delete", "please")

      // Remove all keys associated with `test` type
      dynamoDbBackedMetadata.delete("test_type")

      // Check size
      val rowCount = dynamoDb.getTable(dynamoDbBackedMetadata.metadataTableName).scan().iterator().asScala.size
      rowCount must equalTo(1)
    }

    // backup
    "backup" in {

      // Perform backup
      dynamoDbBackedMetadata.backup("not_to_be_deleted")

      // Now we should have two tables
      dynamoDb.listTables().asScala.toList.size must equalTo(2)
    }


  }

  // Stop server after tests
  step {
    // Wait so can poke
    // Thread.sleep(1000000)
    server.stop()
  }

}

