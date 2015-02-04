package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 12/30/14.
 */
public interface DiscoverFieldsCallback {
  public void notifyFields( List<MongoField> fields );
  public void notifyException( Exception exception );
}
