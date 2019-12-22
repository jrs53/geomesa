/** *********************************************************************
 * Crown Copyright (c) 2017-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * **********************************************************************/

package org.locationtech.geomesa.accumulo.tools.authentication

import java.io.File

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AuthenticationTest extends Specification {

  sequential

  "GeoMesa Accumulo Commands" should {

    // These tests all invoke an ingest command using a mock instance
    // The authentication parameters aren't actually used by the mock instance, but these tests serve to test parameter
    // validation in AccumuloRunner and associated classes.

    val conf = ConfigFactory.load("examples/example1-csv.conf")
    val sft = conf.root().render(ConfigRenderOptions.concise())
    val converter = conf.root().render(ConfigRenderOptions.concise())
    val dataFile = new File(this.getClass.getClassLoader.getResource("examples/example1.csv").getFile)

    // Only test ingest, other commands use the same Accumulo Parameters anyway
    val cmd = Array("ingest")

    // Exhaustively test all combinations of user, password, keytab, tgt

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Absent      |    Absent    |   Absent
    "fail with no authentication params" >> {
      val authArgs = Array("")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Absent      |    Absent    |   Present
    "work with just tgt" >> {
      val authArgs = Array("--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "justtgt", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Absent      |    Present   |   Absent
    "fail with just keytab" >> {
      val authArgs = Array("--keytab", "/will/be/ignored/since/mock")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Absent      |    Present   |   Present
    "fail with both keytab & tgt" >> {
      val authArgs = Array("--keytab", "/will/be/ignored/since/mock", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Present     |    Absent    |   Absent
    "fail with just password" >> {
      val authArgs = Array("--password", "secret")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Present     |    Absent    |   Present
    "fail with password and tgt" >> {
      val authArgs = Array("--password", "secret", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Present     |    Present   |   Absent
    "fail with password and keytab" >> {
      val authArgs = Array("--password", "secret", "--keytab", "/will/be/ignored/since/mock")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Absent  |    Present     |    Present   |   Present
    "fail with password and keytab and tgt" >> {
      val authArgs = Array("--password", "secret", "--keytab", "/will/be/ignored/since/mock", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Absent      |    Absent    |   Absent
    // NOTE: can;t test this since should prompt for password from console :-(

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Absent      |    Absent    |   Present
    "work with user and tgt" >> {
      val authArgs = Array("--user", "me", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandtgt", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))

      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Absent      |    Present   |   Absent
    "work with user and keytab" >> {
      val authArgs = Array("--user", "me", "--keytab", "/will/be/ignored/since/mock")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandkeytab", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))

      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Absent      |    Present   |   Present
    "fail with user and keytab and tgt" >> {
      val authArgs = Array("--user", "me", "--keytab", "/will/be/ignored/since/mock", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Present     |    Absent    |   Absent
    "work with user and password" >> {
      val authArgs = Array("--user", "root", "--password", "secret")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "userandpassword", "--converter", converter, "-s", sft, dataFile.getPath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val features = command.withDataStore(ds => SelfClosingIterator(ds.getFeatureSource("renegades").getFeatures.features).toList)
      features.size mustEqual 3
      features.map(_.get[String]("name")) must containTheSameElementsAs(Seq("Hermione", "Harry", "Severus"))

      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Present     |    Absent    |   Present
    "fail with user and password and tgt" >> {
      val authArgs = Array("--user", "me", "--password", "secret", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Present     |    Present   |   Absent
    "fail with user and password and keytab" >> {
      val authArgs = Array("--user", "me", "--password", "secret", "--keytab", "/will/be/ignored/since/mock")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

    //     User    |    Password    |    Keytab    |    Tgt
    // ------------|----------------|--------------|-----------
    //     Present |    Present     |    Present   |   Present
    "fail with user and password and keytab and tgt" >> {
      val authArgs = Array("--user", "me", "--password", "secret", "--keytab", "/will/be/ignored/since/mock", "--tgt")
      val args = cmd ++ authArgs ++ Array("--instance", "instance", "--zookeepers", "zoo", "--mock", "--catalog", "z", "--converter", converter, "-s", sft, dataFile.getPath)
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand] must throwA[com.beust.jcommander.ParameterException]
      ok
    }

  }

}
