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
    check( "USE_SSL_SOCKET_FACTORY", new BooleanGetter() {
      public boolean get() {
        return meta.isUseSSLSocketFactory();
      }
    } );
    check( "USE_CONNECTION_STRING", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.isUseConnectionString();
      }
    } );
    check( "USE_LEGACY_OPTIONS", new BooleanGetter() {
      @Override
      public boolean get() {
        return meta.isUseLegacyOptions();
      }
    } );
    check( "CONNECTION_STRING", new StringGetter() {
      @Override
      public String get() {
        return meta.getConnectionString();
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
    check( "ALLOWDISKUSE", new BooleanGetter() {
      public boolean get() {
        return meta.isAllowDiskUse();
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
