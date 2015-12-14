/*!
 * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.trans.steps.mongodboutput;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidatorFactory;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ObjectValidator;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoField;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.pentaho.metastore.api.IMetaStore;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MongoDbOutputMetaTest {
  @BeforeClass public static void beforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String
      passwordEncoderPluginID =
      Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test public void testRoundTrips() throws KettleException {
    List<String> commonFields =
      Arrays.asList( "mongo_host", "mongo_port", "use_all_replica_members", "mongo_user", "mongo_password",
        "auth_kerberos", "mongo_db", "mongo_collection", "batch_insert_size", "connect_timeout", "socket_timeout",
        "read_preference", "write_concern", "w_timeout", "journaled_writes", "truncate", "update", "upsert",
        "multi", "modifier_update", "write_retries", "write_retry_delay", "mongo_fields", "mongo_indexes" );
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "mongo_host", "getHostnames" );
    getterMap.put( "mongo_port", "getPort" );
    getterMap.put( "use_all_replica_members", "getUseAllReplicaSetMembers" );
    getterMap.put( "mongo_user", "getAuthenticationUser" );
    getterMap.put( "mongo_password", "getAuthenticationPassword" );
    getterMap.put( "auth_kerberos", "getUseKerberosAuthentication" );
    getterMap.put( "mongo_db", "getDbName" );
    getterMap.put( "mongo_collection", "getCollection" );
    getterMap.put( "journaled_writes", "getJournal" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "mongo_host", "setHostnames" );
    setterMap.put( "mongo_port", "setPort" );
    setterMap.put( "use_all_replica_members", "setUseAllReplicaSetMembers" );
    setterMap.put( "mongo_user", "setAuthenticationUser" );
    setterMap.put( "mongo_password", "setAuthenticationPassword" );
    setterMap.put( "auth_kerberos", "setUseKerberosAuthentication" );
    setterMap.put( "mongo_db", "setDbName" );
    setterMap.put( "mongo_collection", "setCollection" );
    setterMap.put( "batch_insert_size", "setBatchInsertSize" );
    setterMap.put( "journaled_writes", "setJournal" );

    LoadSaveTester tester = new LoadSaveTester( MongoDbOutputMeta.class, commonFields, getterMap, setterMap );

    FieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator( validatorFactory.getName( List.class, MongoField.class ),
      new ListLoadSaveValidator<MongoField>( new ObjectValidator<MongoField>( validatorFactory, MongoField.class,
        Arrays.<String>asList( "m_incomingFieldName", "m_mongoDocPath", "m_useIncomingFieldNameAsMongoFieldName",
          "m_updateMatchField", "m_modifierUpdateOperation", "m_modifierOperationApplyPolicy", "m_JSON",
          "insertNull" ) ) ) );

    validatorFactory.registerValidator( validatorFactory.getName( List.class, MongoIndex.class ),
      new ListLoadSaveValidator<MongoIndex>( new ObjectValidator<MongoIndex>( validatorFactory, MongoIndex.class,
        Arrays.<String>asList( "m_pathToFields", "m_drop", "m_unique", "m_sparse" ) ) ) );

    tester.testXmlRoundTrip();
    tester.testRepoRoundTrip();
  }

  @Test public void testForPDI12155_NotDeprecatedSaveRepMethodImplemented() throws Exception {
    Class[] cArg = { Repository.class, IMetaStore.class, ObjectId.class, ObjectId.class };
    try {
      MongoDbOutputMeta.class.getDeclaredMethod( "saveRep", cArg );
    } catch ( NoSuchMethodException e ) {
      fail( "There is no such a method BUT should be: " + e.getMessage() );
    }
  }

  @Test public void testForPDI12155_DeprecatedSaveRepMethodNotImplemented() throws Exception {
    Class[] cArg = { Repository.class, ObjectId.class, ObjectId.class };
    try {
      Method declaredMethod = MongoDbOutputMeta.class.getDeclaredMethod( "saveRep", cArg );
      fail( "There is a method BUT should not be: " + declaredMethod );
    } catch ( NoSuchMethodException e ) {
      assertTrue( true );
    }
  }
}
