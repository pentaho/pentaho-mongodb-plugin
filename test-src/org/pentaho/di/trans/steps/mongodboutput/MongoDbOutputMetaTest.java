package org.pentaho.di.trans.steps.mongodboutput;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidatorFactory;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ObjectValidator;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoField;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoIndex;

public class MongoDbOutputMetaTest {
  @Test
  public void testRoundTrips() throws KettleException {
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
            Arrays.<String> asList( "m_incomingFieldName", "m_mongoDocPath", "m_useIncomingFieldNameAsMongoFieldName",
                "m_updateMatchField", "m_modifierUpdateOperation", "m_modifierOperationApplyPolicy", "m_JSON" ) ) ) );

    validatorFactory.registerValidator( validatorFactory.getName( List.class, MongoIndex.class ),
        new ListLoadSaveValidator<MongoIndex>( new ObjectValidator<MongoIndex>( validatorFactory, MongoIndex.class,
            Arrays.<String> asList( "m_pathToFields", "m_drop", "m_unique", "m_sparse" ) ) ) );
    
    tester.testXmlRoundTrip();
    tester.testRepoRoundTrip();
  }
}
