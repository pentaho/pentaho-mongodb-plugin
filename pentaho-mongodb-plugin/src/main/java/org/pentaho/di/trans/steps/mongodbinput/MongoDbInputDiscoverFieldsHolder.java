/*!
 * Copyright 2010 - 2021 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
