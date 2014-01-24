package org.pentaho.mongo.wrapper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public class UsernamePasswordMongoClientWrapper extends NoAuthMongoClientWrapper {
  static Class<?> PKG = UsernamePasswordMongoClientWrapper.class;

  private final String user;
  private final String password;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   * 
   * @param meta
   *          the step meta data
   * @param vars
   *          variables to use
   * @param cred
   *          a configured MongoCredential for authentication (or null for no authentication)
   * @param log
   *          for logging
   * @return a configured MongoClient object
   * @throws KettleException
   *           if a problem occurs
   */
  public UsernamePasswordMongoClientWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log )
    throws KettleException {
    super( meta, vars, log );
    user = vars.environmentSubstitute( meta.getAuthenticationUser() );
    password = Encr.decryptPasswordOptionallyEncrypted( vars.environmentSubstitute( meta.getAuthenticationPassword() ) );
  }
  
  public UsernamePasswordMongoClientWrapper( MongoClient mongo, LogChannelInterface log, String user, String password ) {
    super( mongo, log );
    this.user = user;
    this.password = password;
  }

  public String getUser() {
    return user;
  }

  @Override
  protected MongoClient getClient( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log,
      List<ServerAddress> repSet, boolean useAllReplicaSetMembers, MongoClientOptions opts ) throws KettleException {
    try {
      List<MongoCredential> credList = new ArrayList<MongoCredential>();
      credList.add( getCredential( meta, vars ) );
      return ( repSet.size() > 1 || ( useAllReplicaSetMembers && repSet.size() >= 1 ) ? new MongoClient( repSet,
          credList, opts ) : ( repSet.size() == 1 ? new MongoClient( repSet.get( 0 ), credList, opts )
          : new MongoClient( new ServerAddress( "localhost" ), credList, opts ) ) ); //$NON-NLS-1$
    } catch ( UnknownHostException u ) {
      throw new KettleException( u );
    }
  }

  /**
   * Create a credentials object
   * 
   * @param dbName
   *          the name of the database
   * @return a configured MongoCredential object
   */
  protected MongoCredential getCredential( MongoDbMeta meta, VariableSpace vars ) {
    return MongoCredential.createMongoCRCredential( vars.environmentSubstitute( meta.getAuthenticationUser() ), vars
        .environmentSubstitute( meta.getDbName() ), Encr.decryptPasswordOptionallyEncrypted(
        vars.environmentSubstitute( meta.getAuthenticationPassword() ) ).toCharArray() );
  }

  protected DB getDb( String dbName ) throws KettleException {
    try {
      DB result = getMongo().getDB( dbName );
      authenticateWithDb( result );
      return result;
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e );
      }
    }
  }

  protected void authenticateWithDb( DB db ) throws KettleException {
    CommandResult comResult = db.authenticateCommand( user, password.toCharArray() );
    if ( !comResult.ok() ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "MongoUsernamePasswordWrapper.ErrorAuthenticating.Exception", //$NON-NLS-1$
          comResult.getErrorMessage() ) );
    }
  }
}
