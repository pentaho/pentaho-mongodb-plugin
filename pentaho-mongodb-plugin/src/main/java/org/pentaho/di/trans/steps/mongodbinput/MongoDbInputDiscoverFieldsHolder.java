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

import org.pentaho.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 11/10/14.
 */
public class MongoDbInputDiscoverFieldsHolder {
  private static MongoDbInputDiscoverFieldsHolder INSTANCE = new MongoDbInputDiscoverFieldsHolder();
  private final Map<Integer, List<MongoDbInputDiscoverFields>> mongoDbInputDiscoverFieldsMap;
  private MongoDbInputDiscoverFields mongoDbInputDiscoverFields = new MongodbInputDiscoverFieldsImpl();

  private MongoDbInputDiscoverFieldsHolder() {
    mongoDbInputDiscoverFieldsMap = new HashMap<Integer, List<MongoDbInputDiscoverFields>>();
  }

  public static MongoDbInputDiscoverFieldsHolder getInstance() {
    return INSTANCE;
  }

  public MongoDbInputDiscoverFields getMongoDbInputDiscoverFields() {
    return mongoDbInputDiscoverFields;
  }

  public void implAdded( MongoDbInputDiscoverFields mongoDbInputDiscoverFields, Map properties ) {
    synchronized ( mongoDbInputDiscoverFieldsMap ) {
      Integer ranking = (Integer) properties.get( "service.ranking" );
      if ( ranking == null ) {
        ranking = 0;
      }
      List<MongoDbInputDiscoverFields> mongoDbInputDiscoverFieldsList = mongoDbInputDiscoverFieldsMap.get( ranking );
      if ( mongoDbInputDiscoverFieldsList == null ) {
        mongoDbInputDiscoverFieldsList = new ArrayList<MongoDbInputDiscoverFields>();
        mongoDbInputDiscoverFieldsMap.put( ranking, mongoDbInputDiscoverFieldsList );
      }
      mongoDbInputDiscoverFieldsList.add( mongoDbInputDiscoverFields );
      updateField();
    }
  }

  public void implRemoved( MongoDbInputDiscoverFields mongoDbInputDiscoverFields, Map properties ) {
    synchronized ( mongoDbInputDiscoverFieldsMap ) {
      Integer ranking = (Integer) properties.get( "service.ranking" );
      if ( ranking == null ) {
        ranking = 0;
      }
      List<MongoDbInputDiscoverFields> mongoDbInputDiscoverFieldsList = mongoDbInputDiscoverFieldsMap.get( ranking );
      if ( mongoDbInputDiscoverFieldsList != null ) {
        mongoDbInputDiscoverFieldsList.remove( mongoDbInputDiscoverFields );
        if ( mongoDbInputDiscoverFieldsList.size() == 0 ) {
          mongoDbInputDiscoverFieldsMap.remove( ranking );
        }
        updateField();
      }
    }
  }

  private void updateField() {
    List<Integer> keys = new ArrayList<Integer>( mongoDbInputDiscoverFieldsMap.keySet() );
    if ( keys.size() == 0 ) {
      mongoDbInputDiscoverFields = null;
    } else {
      Collections.sort( keys );
      mongoDbInputDiscoverFields = mongoDbInputDiscoverFieldsMap.get( keys.get( keys.size() - 1 ) ).get( 0 );
    }
  }
}
