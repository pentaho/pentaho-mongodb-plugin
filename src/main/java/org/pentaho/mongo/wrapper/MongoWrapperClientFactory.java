package org.pentaho.mongo.wrapper;

import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/22/14.
 */
public interface MongoWrapperClientFactory {
  public MongoClientWrapper createMongoClientWrapper( MongoProperties props, MongoUtilLogger log ) throws MongoDbException;
}
