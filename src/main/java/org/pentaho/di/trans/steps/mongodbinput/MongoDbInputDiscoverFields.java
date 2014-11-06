package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 11/4/14.
 */
public interface MongoDbInputDiscoverFields {
    public List<MongoField> discoverFields( MongoProperties.Builder properties, String db, final String collection, final String query, final String fields,
                                                   final boolean isPipeline, final int docsToSample ) throws KettleException;
}
