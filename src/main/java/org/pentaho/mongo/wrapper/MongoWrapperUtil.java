package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/7/14.
 */
public class MongoWrapperUtil {
  private static MongoWrapperClientFactory mongoWrapperClientFactory = new MongoWrapperClientFactory() {
    @Override public MongoClientWrapper createMongoClientWrapper( MongoProperties props, MongoUtilLogger log )
      throws MongoDbException {
      return MongoClientWrapperFactory.createMongoClientWrapper( props, log );
    }
  };

  protected static void setMongoWrapperClientFactory( MongoWrapperClientFactory mongoWrapperClientFactory ) {
    MongoWrapperUtil.mongoWrapperClientFactory = mongoWrapperClientFactory;
  }

  protected static MongoWrapperClientFactory getMongoWrapperClientFactory() {
    return mongoWrapperClientFactory;
  }

  public static MongoClientWrapper createMongoClientWrapper( MongoDbMeta mongoDbMeta, VariableSpace vars,
                                                             LogChannelInterface log ) throws MongoDbException {
    MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.HOST, vars.environmentSubstitute( mongoDbMeta.getHostnames() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.PORT, vars.environmentSubstitute( mongoDbMeta.getPort() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.DBNAME, vars.environmentSubstitute( mongoDbMeta.getDbName() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.connectTimeout, mongoDbMeta.getConnectTimeout() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.socketTimeout, mongoDbMeta.getSocketTimeout() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.readPreference, mongoDbMeta.getReadPreference() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.writeConcern, mongoDbMeta.getWriteConcern() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.wTimeout, mongoDbMeta.getWTimeout() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.JOURNALED, Boolean.toString( mongoDbMeta.getJournal() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USE_ALL_REPLICA_SET_MEMBERS,
      Boolean.toString( mongoDbMeta.getUseAllReplicaSetMembers() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USERNAME, mongoDbMeta.getAuthenticationUser() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.PASSWORD, mongoDbMeta.getAuthenticationPassword() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USE_KERBEROS,
      Boolean.toString( mongoDbMeta.getUseKerberosAuthentication() ) );
    if ( mongoDbMeta.getReadPrefTagSets() != null ) {
      StringBuilder tagSet = new StringBuilder();
      for ( String tag : mongoDbMeta.getReadPrefTagSets() ) {
        tagSet.append( tag );
        tagSet.append( "," );
      }
      // Remove trailing comma
      if ( tagSet.length() > 0 ) {
        tagSet.setLength( tagSet.length() - 1 );
      }
      setIfNotNullOrEmpty( propertiesBuilder, MongoProp.tagSet, tagSet.toString() );
    }
    return mongoWrapperClientFactory
      .createMongoClientWrapper( propertiesBuilder.build(), new KettleMongoUtilLogger( log ) );
  }

  private static void setIfNotNullOrEmpty( MongoProperties.Builder builder, MongoProp prop, String value ) {
    if ( value != null && value.trim().length() > 0 ) {
      builder.set( prop, value );
    }
  }
}
