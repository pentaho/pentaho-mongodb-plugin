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
package org.pentaho.mongo.wrapper;

import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/22/14.
 */
public interface MongoWrapperClientFactory {
  MongoClientWrapper createMongoClientWrapper( MongoProperties props, MongoUtilLogger log ) throws MongoDbException;

  MongoClientWrapper createConnectionStringMongoClientWrapper( String connectionString, MongoUtilLogger log ) throws MongoDbException;
}
