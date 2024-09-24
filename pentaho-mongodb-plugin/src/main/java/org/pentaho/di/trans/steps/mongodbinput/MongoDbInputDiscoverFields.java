/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 11/4/14.
 */
public interface MongoDbInputDiscoverFields {
  List<MongoField> discoverFields( MongoProperties.Builder properties, String db, String collection,
                                   String query, String fields,
                                   boolean isPipeline, int docsToSample, MongoDbInputMeta step, VariableSpace vars )
    throws KettleException;

  void discoverFields( MongoProperties.Builder properties, String db, String collection, String query,
                       String fields,
                       boolean isPipeline, int docsToSample, MongoDbInputMeta step,
                       VariableSpace vars, DiscoverFieldsCallback discoverFieldsCallback ) throws KettleException;
}
