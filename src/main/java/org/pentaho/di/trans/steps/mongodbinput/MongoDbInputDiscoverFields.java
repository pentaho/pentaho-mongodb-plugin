package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 11/4/14.
 */
public interface MongoDbInputDiscoverFields {
  public List<MongoField> discoverFields( MongoProperties.Builder properties, String db, String collection,
                                          String query, String fields,
                                          boolean isPipeline, int docsToSample, MongoDbInputMeta step )
    throws KettleException;

  public void discoverFields( MongoProperties.Builder properties, String db, String collection, String query,
                              String fields,
                              boolean isPipeline, int docsToSample, MongoDbInputMeta step,
                              DiscoverFieldsCallback discoverFieldsCallback ) throws KettleException;
}
