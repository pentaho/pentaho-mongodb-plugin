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


package org.pentaho.di.trans.steps.mongodbinput;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.AnalysisContext;
import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.model.BaseResourceInfo;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

/**
 * User: RFellows Date: 3/10/15
 */
public class MongoDbResourceInfo extends BaseResourceInfo implements IExternalResourceInfo {

  public static final String JSON_PROPERTY_PORT = "port";
  public static final String JSON_PROPERTY_USERNAME = "username";
  public static final String JSON_PROPERTY_PASSWORD = "password";
  public static final String JSON_PROPERTY_DATABASE_NAME = "databaseName";
  public static final String JSON_PROPERTY_CONNECTION_TIMEOUT = "connectionTimeout";
  public static final String JSON_PROPERTY_HOST_NAMES = "hostNames";
  public static final String JSON_PROPERTY_SOCKET_TIMEOUT = "socketTimeout";
  public static final String JSON_PROPERTY_USE_ALL_REPLICA_SET_MEMBERS = "useAllReplicaSetMembers";
  public static final String JSON_PROPERTY_USE_KERBEROS_AUTHENTICATION = "useKerberosAuthentication";
  public static final String JSON_PROPERTY_COLLECTION = "collection";

  private String database;
  private String port;
  private String hostNames;
  private String user;
  private String password;
  private boolean useAllReplicaSetMembers;
  private boolean useKerberosAuthentication;
  private String connectTimeout;
  private String socketTimeout;
  private String collection;

  public MongoDbResourceInfo( MongoDbMeta mongoDbMeta ) {
    this( mongoDbMeta, new AnalysisContext( DictionaryConst.CONTEXT_RUNTIME ) );
  }

  public MongoDbResourceInfo( MongoDbMeta mongoDbMeta, IAnalysisContext context ) {
    setName( substituteIfNeeded( mongoDbMeta.getDbName(), mongoDbMeta, context ) );
    setDatabase( substituteIfNeeded( mongoDbMeta.getDbName(), mongoDbMeta, context ) );
    setPort( substituteIfNeeded( mongoDbMeta.getPort(), mongoDbMeta, context ) );
    setHostNames( substituteIfNeeded( mongoDbMeta.getHostnames(), mongoDbMeta, context ) );
    setUser( substituteIfNeeded( mongoDbMeta.getAuthenticationUser(), mongoDbMeta, context ) );
    setPassword( substituteIfNeeded( mongoDbMeta.getAuthenticationPassword(), mongoDbMeta, context ) );
    setUseAllReplicaSetMembers( mongoDbMeta.getUseAllReplicaSetMembers() );
    setUseKerberosAuthentication( mongoDbMeta.getUseKerberosAuthentication() );
    setConnectTimeout( substituteIfNeeded( mongoDbMeta.getConnectTimeout(), mongoDbMeta, context ) );
    setSocketTimeout( substituteIfNeeded( mongoDbMeta.getSocketTimeout(), mongoDbMeta, context ) );
    setCollection( substituteIfNeeded( mongoDbMeta.getCollection(), mongoDbMeta, context ) );
  }

  public MongoDbResourceInfo( String hostNames, String port, String database ) {
    setHostNames( hostNames );
    setPort( port );
    setDatabase( database );
  }

  private String substituteIfNeeded( String value, MongoDbMeta meta, IAnalysisContext context ) {
    String contextName = context != null ? context.getContextName() : "";
    String ret = contextName.equals( DictionaryConst.CONTEXT_RUNTIME )
      ? meta.getParentStepMeta().getParentTransMeta().environmentSubstitute( value ) : value;
    return ret;
  }

  @Override
  public String getType() {
    return "MongoDbResource";
  }

  @JsonProperty( JSON_PROPERTY_CONNECTION_TIMEOUT )
  public String getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout( String connectTimeout ) {
    this.connectTimeout = connectTimeout;
  }

  @JsonProperty( JSON_PROPERTY_DATABASE_NAME )
  public String getDatabase() {
    return database;
  }

  public void setDatabase( String database ) {
    this.database = database;
  }

  @JsonProperty( JSON_PROPERTY_HOST_NAMES )
  public String getHostNames() {
    return hostNames;
  }

  public void setHostNames( String hostNames ) {
    this.hostNames = hostNames;
  }

  @JsonProperty( JSON_PROPERTY_PORT )
  public String getPort() {
    return port;
  }

  public void setPort( String port ) {
    this.port = port;
  }

  @JsonProperty( JSON_PROPERTY_SOCKET_TIMEOUT )
  public String getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout( String socketTimeout ) {
    this.socketTimeout = socketTimeout;
  }

  @JsonProperty( JSON_PROPERTY_USE_ALL_REPLICA_SET_MEMBERS )
  public boolean isUseAllReplicaSetMembers() {
    return useAllReplicaSetMembers;
  }

  public void setUseAllReplicaSetMembers( boolean useAllReplicaSetMembers ) {
    this.useAllReplicaSetMembers = useAllReplicaSetMembers;
  }

  @JsonProperty( JSON_PROPERTY_USE_KERBEROS_AUTHENTICATION )
  public boolean isUseKerberosAuthentication() {
    return useKerberosAuthentication;
  }

  public void setUseKerberosAuthentication( boolean useKerberosAuthentication ) {
    this.useKerberosAuthentication = useKerberosAuthentication;
  }

  @JsonProperty( JSON_PROPERTY_USERNAME )
  public String getUser() {
    return user;
  }

  public void setUser( String user ) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  @JsonProperty( JSON_PROPERTY_PASSWORD )
  public void setPassword( String password ) {
    this.password = Encr.decryptPasswordOptionallyEncrypted( password );
  }

  @JsonProperty( JSON_PROPERTY_PASSWORD )
  protected String getEncryptedPassword() {
    if ( StringUtils.isEmpty( password ) ) {
      return StringUtils.EMPTY;
    }
    // Need "Encrypted prefix for decryptPasswordOptionallyEncrypted() to operate properly
    return Encr.PASSWORD_ENCRYPTED_PREFIX + Encr.encryptPassword( password );
  }

  @JsonProperty( JSON_PROPERTY_COLLECTION )
  public String getCollection() {
    return collection;
  }

  public void setCollection( String collection ) {
    this.collection = collection;
  }

  @Override
  public void cleanupSensitiveData() {
    password = null;
  }
}
