/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.mongo.wrapper;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.encryption.Encr;
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
    @Override public MongoClientWrapper createConnectionStringMongoClientWrapper( String connectionString, MongoUtilLogger log )
            throws MongoDbException {
      return MongoClientWrapperFactory.createConnectionStringMongoClientWrapper( connectionString, log );
    }
  };

  public static void setMongoWrapperClientFactory( MongoWrapperClientFactory mongoWrapperClientFactory ) {
    MongoWrapperUtil.mongoWrapperClientFactory = mongoWrapperClientFactory;
  }

  @VisibleForTesting
  public static MongoWrapperClientFactory getMongoWrapperClientFactory() {
    return mongoWrapperClientFactory;
  }

  public static MongoClientWrapper createMongoClientWrapper( MongoDbMeta mongoDbMeta, VariableSpace vars,
                                                             LogChannelInterface log ) throws MongoDbException {
    if ( mongoDbMeta.isUseConnectionString() ) {
      return mongoWrapperClientFactory.createConnectionStringMongoClientWrapper(
              Encr.decryptPasswordOptionallyEncrypted( vars.environmentSubstitute( mongoDbMeta.getConnectionString() ) ), new KettleMongoUtilLogger( log ) );
    } else {
      MongoProperties.Builder propertiesBuilder = createPropertiesBuilder( mongoDbMeta, vars );

      return mongoWrapperClientFactory
              .createMongoClientWrapper( propertiesBuilder.build(), new KettleMongoUtilLogger( log ) );
    }
  }

  public static MongoProperties.Builder createPropertiesBuilder( MongoDbMeta mongoDbMeta, VariableSpace vars ) {
    MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();

    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.HOST, vars.environmentSubstitute( mongoDbMeta.getHostnames() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.PORT, vars.environmentSubstitute( mongoDbMeta.getPort() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.DBNAME, vars.environmentSubstitute( mongoDbMeta.getDbName() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.connectTimeout, vars.environmentSubstitute( mongoDbMeta.getConnectTimeout() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.socketTimeout, vars.environmentSubstitute( mongoDbMeta.getSocketTimeout() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.readPreference, mongoDbMeta.getReadPreference() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.writeConcern, mongoDbMeta.getWriteConcern() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.wTimeout, mongoDbMeta.getWTimeout() );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.JOURNALED, Boolean.toString( mongoDbMeta.getJournal() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USE_ALL_REPLICA_SET_MEMBERS,
        Boolean.toString( mongoDbMeta.getUseAllReplicaSetMembers() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.AUTH_DATABASE, vars.environmentSubstitute( mongoDbMeta.getAuthenticationDatabaseName() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USERNAME, vars.environmentSubstitute( mongoDbMeta.getAuthenticationUser() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.PASSWORD, vars.environmentSubstitute( mongoDbMeta.getAuthenticationPassword() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.AUTH_MECHA, mongoDbMeta.getAuthenticationMechanism()  );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.USE_KERBEROS,
        Boolean.toString( mongoDbMeta.getUseKerberosAuthentication() ) );
    setIfNotNullOrEmpty( propertiesBuilder, MongoProp.useSSL,
        Boolean.toString( mongoDbMeta.isUseSSLSocketFactory() ) );
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

    return propertiesBuilder;
  }

  public static MongoClientWrapper createMongoClientWrapper( MongoProperties.Builder properties, LogChannelInterface log )
    throws MongoDbException {
    return mongoWrapperClientFactory
        .createMongoClientWrapper( properties.build(), new KettleMongoUtilLogger( log ) );
  }

  private static void setIfNotNullOrEmpty( MongoProperties.Builder builder, MongoProp prop, String value ) {
    if ( value != null && value.trim().length() > 0 ) {
      boolean isPassword = MongoProp.PASSWORD.equals( prop );
      if ( isPassword ) {
        value = Encr.decryptPasswordOptionallyEncrypted( value );
      }
      builder.set( prop, value );
    }
  }
}
