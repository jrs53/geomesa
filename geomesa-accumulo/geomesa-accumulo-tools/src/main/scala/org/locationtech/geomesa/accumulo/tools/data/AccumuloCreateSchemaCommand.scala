/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.Parameters
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.data.{CreateSchemaCommand, CreateSchemaParams}

class AccumuloCreateSchemaCommand extends CreateSchemaCommand[AccumuloDataStore] with AccumuloDataStoreCommand {
  override val params = new AccumuloCreateSchemaParams()
}

@Parameters(commandDescription = "Create a GeoMesa feature type")
class AccumuloCreateSchemaParams extends CreateSchemaParams with AccumuloDataStoreParams
