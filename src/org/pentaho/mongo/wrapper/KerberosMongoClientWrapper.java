package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.KettleKerberosHelper;
import org.pentaho.mongo.wrapper.collection.KerberosMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

public class KerberosMongoClientWrapper extends UsernamePasswordMongoClientWrapper {
  private final AuthContext authContext;

  public KerberosMongoClientWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log ) throws KettleException {
    super( meta, vars, log );
    authContext = new AuthContext( KettleKerberosHelper.login( vars, getUser() ) );
  }

  public KerberosMongoClientWrapper( MongoClient client, LogChannelInterface log, String username, AuthContext authContext ) {
    super( client, log, username, null );
    this.authContext = authContext;
  }

  @Override
  protected MongoCredential getCredential( MongoDbMeta meta, VariableSpace vars ) {
    return MongoCredential.createGSSAPICredential( vars.environmentSubstitute( meta.getAuthenticationUser() ) );
  }

  @Override
  protected void authenticateWithDb( DB db ) throws KettleException {
    // noop
  }

  @Override
  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return KerberosInvocationHandler.wrap( MongoCollectionWrapper.class, authContext,
        new KerberosMongoCollectionWrapper( collection, authContext ) );
  }

  public AuthContext getAuthContext() {
    return authContext;
  }
}
