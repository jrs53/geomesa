/** *********************************************************************
 * Crown Copyright (c) 2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * ********************************************************************* */

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.dynamodb.data.DynamoDbDataStoreFactory.Params.{CatalogParam, EndpointParam, RegionParam}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DynamoDbDataStoreFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  // TODO: consider port collisions
  private val DYNAMODB_PORT = "8000"

  "DynamoDbDataStoreFactory" should {

    "connect to local DynamoDB instance" in {

      // DynamoDB Local needs native sqlite4java libs; maven-dependency-plugin should put them here
      System.setProperty("sqlite4java.library.path", "target/test-classes/native-libs")

      // Create local DynamoDB for this test
      val server = ServerRunner.createServerFromCommandLineArgs(Array("-inMemory", "-port", DYNAMODB_PORT))
      server.start()

      val params = Map(
        EndpointParam.key -> s"http://localhost:$DYNAMODB_PORT",
        RegionParam.key -> "eu-west-2",
        CatalogParam.key -> "test"
      ).asJava

      // Param values are good...
      DynamoDbDataStoreFactory.canProcess(params) must beTrue

      // ...and can use them to create datastore
      val ds = DataStoreFinder.getDataStore(params)
      try {
        ds must beAnInstanceOf[DynamoDbDataStore]
      }
      finally {

        // Stop local DynamoDB
        server.stop()

        // Cleanly dispose datastore
        if (ds != null) {
          ds.dispose()
        }

      }

    }

    "not connect to an invalid non-cloud endpoint" in {

      val params = Map(
        EndpointParam.key -> s"http://www.example.com",
        RegionParam.key -> "eu-west-2",
        CatalogParam.key -> "test"
      ).asJava

      // Params values are okay (until we try to use them)
      DynamoDbDataStoreFactory.canProcess(params) must beTrue

      // ...but can't connect to the specified DynamoDB Local instance
      val ds = DataStoreFinder.getDataStore(params)
      ds must be(null)

    }

    "not connect to an invalid region" in {
      val params = Map(
        EndpointParam.key -> s"http://www.example.com",
        RegionParam.key -> "not-a-region",
        CatalogParam.key -> "test"
      ).asJava

      // Region param should be invalid
      DynamoDbDataStoreFactory.canProcess(params) must beFalse
    }


  }
}

