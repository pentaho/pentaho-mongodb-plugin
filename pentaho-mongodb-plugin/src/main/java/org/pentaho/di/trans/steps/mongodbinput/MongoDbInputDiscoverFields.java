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
