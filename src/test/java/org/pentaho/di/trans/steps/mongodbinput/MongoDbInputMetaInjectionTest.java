/*!
 * Copyright 2010 - 2016 Pentaho Corporation.  All rights reserved.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMetaInjectionTest;

/**
 * MDI test for MongoDbInput.
 */
public class MongoDbInputMetaInjectionTest extends BaseMetadataInjectionTest<MongoDbInputMeta> {

  private LogChannelInterfaceFactory oldLogChannelInterfaceFactory;

  @Before
  public void setup() {
    oldLogChannelInterfaceFactory = KettleLogStore.getLogChannelInterfaceFactory();
    MongoDbOutputMetaInjectionTest.setKettleLogFactoryWithMock();
    setup( new MongoDbInputMeta() );
  }

  @After
  public void tearDown() {
    KettleLogStore.setLogChannelInterfaceFactory( oldLogChannelInterfaceFactory );
  }

  @Test
  public void test() throws Exception {
    check( "HOSTNAME", new StringGetter() {
      public String get() {
        return meta.getHostnames();
      }
    } );
    check( "JSON_FIELD", new StringGetter() {
      public String get() {
        return meta.getFieldsName();
      }
    } );
    check( "JSON_QUERY", new StringGetter() {
      public String get() {
        return meta.getJsonQuery();
      }
    } );
    check( "PORT", new StringGetter() {
      public String get() {
        return meta.getPort();
      }
    } );
    check( "DATABASE_NAME", new StringGetter() {
      public String get() {
        return meta.getDbName();
      }
    } );
    check( "COLLECTION", new StringGetter() {
      public String get() {
        return meta.getCollection();
      }
    } );
    check( "AUTH_DATABASE", new StringGetter() {
      public String get() {
        return meta.getAuthenticationDatabaseName();
      }
    } );
    check( "AUTH_USERNAME", new StringGetter() {
      public String get() {
        return meta.getAuthenticationUser();
      }
    } );
    check( "AUTH_PASSWORD", new StringGetter() {
      public String get() {
        return meta.getAuthenticationPassword();
      }
    } );
    check( "AUTH_MECHANISM", new StringGetter() {
      public String get() {
        return meta.getAuthenticationMechanism();
      }
    } );
    check( "AUTH_KERBEROS", new BooleanGetter() {
      public boolean get() {
        return meta.getUseKerberosAuthentication();
      }
    } );
    check( "TIMEOUT_CONNECTION", new StringGetter() {
      public String get() {
        return meta.getConnectTimeout();
      }
    } );
    check( "TIMEOUT_SOCKET", new StringGetter() {
      public String get() {
        return meta.getSocketTimeout();
      }
    } );
    check( "READ_PREFERENCE", new StringGetter() {
      public String get() {
        return meta.getReadPreference();
      }
    } );
    check( "USE_ALL_REPLICA_SET_MEMBERS", new BooleanGetter() {
      public boolean get() {
        return meta.getUseAllReplicaSetMembers();
      }
    } );
    check( "TAG_SET", new StringGetter() {
      public String get() {
        return meta.getReadPrefTagSets().get( 0 );
      }
    } );
    check( "JSON_OUTPUT_FIELD", new StringGetter() {
      public String get() {
        return meta.getJsonFieldName();
      }
    } );
    check( "AGG_PIPELINE", new BooleanGetter() {
      public boolean get() {
        return meta.getQueryIsPipeline();
      }
    } );
    check( "OUTPUT_JSON", new BooleanGetter() {
      public boolean get() {
        return meta.getOutputJson();
      }
    } );
    check( "EXECUTE_FOR_EACH_ROW", new BooleanGetter() {
      public boolean get() {
        return meta.getExecuteForEachIncomingRow();
      }
    } );
    check( "FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getMongoFields().get( 0 ).m_fieldName;
      }
    } );
    check( "FIELD_PATH", new StringGetter() {
      public String get() {
        return meta.getMongoFields().get( 0 ).m_fieldPath;
      }
    } );
    check( "FIELD_TYPE", new StringGetter() {
      public String get() {
        return meta.getMongoFields().get( 0 ).m_kettleType;
      }
    } );
    check( "FIELD_INDEXED", new StringGetter() {
      public String get() {
        return meta.getMongoFields().get( 0 ).m_indexedVals.get( 0 );
      }
    } );
    check( "FIELD_ARRAY_INDEX", new StringGetter() {
      public String get() {
        return meta.getMongoFields().get( 0 ).m_arrayIndexInfo;
      }
    } );
    check( "FIELD_PERCENTAGE", new IntGetter() {
      public int get() {
        return meta.getMongoFields().get( 0 ).m_percentageOfSample;
      }
    } );
    check( "FIELD_DISPARATE_TYPES", new BooleanGetter() {
      public boolean get() {
        return meta.getMongoFields().get( 0 ).m_disparateTypes;
      }
    } );
  }
}
