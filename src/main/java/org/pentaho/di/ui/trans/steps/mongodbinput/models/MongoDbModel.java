/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.di.ui.trans.steps.mongodbinput.models;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.parameters.DuplicateParamException;
import org.pentaho.di.core.parameters.NamedParams;
import org.pentaho.di.core.parameters.NamedParamsDefault;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputDialog;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.stereotype.Bindable;
import org.pentaho.ui.xul.util.AbstractModelList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class MongoDbModel extends XulEventSourceAdapter {

  private String hostname;

  private String port;

  private String dbName;
  private Vector<String> dbNames = new Vector<String>();

  private String collection;
  private Vector<String> collections = new Vector<String>();

  private String fieldsQuery;

  private String authenticationDatabaseName;

  private String authenticationUser;

  private String authenticationPassword;

  private String jsonQuery;

  private boolean m_aggPipeline = false;

  private boolean m_useAllReplicaSetMembers = false;

  private String m_connectTimeout = ""; // default - never time out

  private String m_socketTimeout = ""; // default - never time out

  private boolean m_kerberos = false;

  /**
   * primary, primaryPreferred, secondary, secondaryPreferred, nearest
   */
  private String m_readPreference = NamedReadPreference.PRIMARY.getName();

  private static final String AUTO_GENERATED_PARAMETER = "AUTO.GENERATED.PARAMETER";

  private AbstractModelList<MongoDocumentField> fields = new AbstractModelList<MongoDocumentField>();

  private AbstractModelList<MongoTag> tags = new AbstractModelList<MongoTag>();

  private MongoDbInputMeta mongo;

  private LogChannel log;

  public MongoDbModel( MongoDbInputMeta mongo ) {
    super();
    this.mongo = mongo;
    log = new LogChannel( this.mongo );
    initialize( this.mongo );
  }

  public boolean validate() {
    boolean valid = false;

    valid = ( !StringUtil.isEmpty( hostname ) )
        //   (!StringUtil.isEmpty(port)) &&     // port can be empty; MongoDb will assume 27017
        && ( !StringUtil.isEmpty( dbName ) ) && ( !StringUtil.isEmpty( collection ) ) && ( fields.size() > 0 );

    firePropertyChange( "validate", null, valid );
    return valid;
  }

  /**
   * @return the hostnames (comma separated: host:<port>)
   */
  public String getHostnames() {
    return hostname;
  }

  /**
   * @param hostname the hostnames to set (comma separated: host:<port>)
   */
  public void setHostnames( String hostname ) {
    String prevVal = this.hostname;
    this.hostname = hostname;

    firePropertyChange( "hostnames", prevVal, hostname );
    validate();
  }

  /**
   * @return the port. This is a port to use for all hostnames (avoids having to
   * specify the same port for each hostname in the hostnames list
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port. This is a port to use for all hostnames (avoids
   *             having to specify the same port for each hostname in the hostnames
   *             list
   */
  public void setPort( String port ) {
    String prevVal = this.port;
    this.port = port;

    firePropertyChange( "port", prevVal, port );
    validate();
  }

  /**
   * @return the dbName
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setDbName( String dbName ) {
    String prevVal = this.dbName;
    this.dbName = dbName;

    firePropertyChange( "database", prevVal, dbName == null ? "" : dbName );
  }

  public Collection<String> getDbNames() {
    return dbNames;
  }

  public void setDbNames( Vector<String> dbs ) {
    Collection<String> prevVal = this.dbNames;
    this.dbNames = dbs;

    // add the current selection, even if not in the list...
    // users are allowed to manually add database names
    // TODO: check behavior in Spoon...

    if ( !Const.isEmpty( dbName ) ) {
      if ( !dbNames.contains( dbName ) ) {
        dbNames.add( dbName );
      }
    }

    firePropertyChange( "database", prevVal, dbs );
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fieldsQuery;
  }

  /**
   * @param fields a field name to set
   */
  public void setFieldsName( String fields ) {
    String prevVal = this.fieldsQuery;
    this.fieldsQuery = fields;

    firePropertyChange( "fieldsQuery", prevVal, fields );
  }

  /**
   * @return the collection
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection the collection to set
   */
  public void setCollection( String collection ) {
    String prevVal = this.collection;
    this.collection = collection;

    firePropertyChange( "collection", prevVal, collection == null ? "" : collection );
  }

  public Collection<String> getCollections() {
    return collections;
  }

  public void setCollections( Vector<String> collections ) {
    Collection<String> prevVal = this.collections;
    this.collections = collections;

    // add the current selection, even if not in the list...
    // users are allowed to manually add collection names
    // TODO: check behavior in Spoon...

    if ( !Const.isEmpty( collection ) ) {
      if ( !collections.contains( collection ) ) {
        collections.add( collection );
      }
    }

    firePropertyChange( "collection", prevVal, collections );
  }

  public String getAuthenticationDatabaseName() {
    return authenticationDatabaseName;
  }

  public void setAuthenticationDatabaseName(String authenticationDatabaseName) {
    String prevVal = this.authenticationDatabaseName;
    this.authenticationDatabaseName = authenticationDatabaseName;

    firePropertyChange( "authenticationDatabaseName", prevVal, authenticationDatabaseName );
  }

  /**
   * @return the authenticationUser
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /**
   * @param authenticationUser the authenticationUser to set
   */
  public void setAuthenticationUser( String authenticationUser ) {
    String prevVal = this.authenticationUser;
    this.authenticationUser = authenticationUser;

    firePropertyChange( "authenticationUser", prevVal, authenticationUser );
  }

  /**
   * @return the authenticationPassword
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /**
   * @param authenticationPassword the authenticationPassword to set
   */
  public void setAuthenticationPassword( String authenticationPassword ) {
    String prevVal = this.authenticationPassword;
    this.authenticationPassword = authenticationPassword;

    firePropertyChange( "authenticationPassword", prevVal, authenticationPassword );
  }

  /**
   * @return the jsonQuery
   */
  public String getJsonQuery() {
    return jsonQuery;
  }

  /**
   * @param jsonQuery the jsonQuery to set
   */
  @Bindable public void setJsonQuery( String jsonQuery ) {
    String prevVal = this.jsonQuery;
    this.jsonQuery = jsonQuery;

    firePropertyChange( "jsonQuery", prevVal, jsonQuery );
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   *
   * @param q true if the supplied query is a pipeline specification
   *          m_aggPipeline = q;
   */
  public void setQueryIsPipeline( boolean q ) {
    Boolean prevVal = new Boolean( this.m_aggPipeline );
    m_aggPipeline = q;

    firePropertyChange( "m_aggPipeline", prevVal, new Boolean( q ) );
  }

  /**
   * Get whether the supplied query is actually a pipeline specification
   *
   * @true true if the supplied query is a pipeline specification
   */
  public boolean getQueryIsPipeline() {
    return m_aggPipeline;
  }

  /**
   * Set  whether to include all members in the replica set for querying
   */
  public void setUseAllReplicaMembers( boolean u ) {
    Boolean prevVal = new Boolean( this.m_useAllReplicaSetMembers );
    m_useAllReplicaSetMembers = u;

    firePropertyChange( "m_useAllReplicaSetMembers", prevVal, new Boolean( u ) );
  }

  /**
   * Set whether to use kerberos authentication
   *
   * @param k true if kerberos is to be used
   */
  public void setUseKerberosAuthentication( boolean k ) {
    Boolean prevVal = new Boolean( this.m_kerberos );
    m_kerberos = k;

    firePropertyChange( "m_kerberos", prevVal, new Boolean( k ) );
  }

  /**
   * Get whether to use kerberos authentication
   *
   * @return true if kerberos is to be used
   */
  public boolean getUseKerberosAuthentication() {
    return m_kerberos;
  }

  /**
   * Get whether to include all members in the replica set for querying
   */
  public boolean getUseAllReplicaMembers() {
    return m_useAllReplicaSetMembers;
  }

  /**
   * Set the connection timeout. The default is never timeout
   *
   * @param to the connection timeout in milliseconds
   */
  public void setConnectTimeout( String to ) {
    String prevVal = this.m_connectTimeout;
    m_connectTimeout = to;

    firePropertyChange( "connectTimeout", prevVal, to );
  }

  /**
   * Get the connection timeout. The default is never timeout
   *
   * @return the connection timeout in milliseconds
   */
  public String getConnectTimeout() {
    return m_connectTimeout;
  }

  /**
   * Set the number of milliseconds to attempt a send or receive on a socket
   * before timing out.
   *
   * @param so the number of milliseconds before socket timeout
   */
  public void setSocketTimeout( String so ) {
    String prevVal = this.m_socketTimeout;
    m_socketTimeout = so;

    firePropertyChange( "socketTimeout", prevVal, so );
  }

  /**
   * Get the number of milliseconds to attempt a send or receive on a socket
   * before timing out.
   *
   * @return the number of milliseconds before socket timeout
   */
  public String getSocketTimeout() {
    return m_socketTimeout;
  }

  /**
   * Set the read preference to use - primary, primaryPreferred, secondary,
   * secondaryPreferred or nearest.
   *
   * @param preference the read preference to use
   */
  public void setReadPreference( String preference ) {
    String prevVal = this.m_readPreference;
    m_readPreference = Const.isEmpty( preference ) ? NamedReadPreference.PRIMARY.getName() : preference;

    firePropertyChange( "readPreference", prevVal, preference );
  }

  /**
   * Get the read preference to use - primary, primaryPreferred, secondary,
   * secondaryPreferred or nearest.
   *
   * @return the read preference to use
   */
  public String getReadPreference() {
    return m_readPreference;
  }

  public AbstractModelList<MongoDocumentField> getFields() {
    return fields;
  }

  public void save() {
    saveMeta( mongo );

    List<String> variablesUsed = new ArrayList<String>();

    StringUtil.getUsedVariables( mongo.getJsonQuery(), variablesUsed, true );
    TransMeta trans = mongo.getParentStepMeta().getParentTransMeta();

    for ( String variable : variablesUsed ) {

      try {

        // The description is a flag telling us that this parameter was not added by the user, 
        // but auto generated by the system... important for managing parameterization within 
        // the embedded datasources...

        trans.addParameterDefinition( StringUtil.getVariableName( variable ), "", AUTO_GENERATED_PARAMETER );
      } catch ( DuplicateParamException e ) {
        // this is GOOD ... we do not want duplicates...
        log.logBasic( "Failed attempt to add duplicate variable ".concat( variable ) );
      }
    }

    String[] parametersAdded = trans.listParameters();
    NamedParams params = new NamedParamsDefault();
    String description = null;

    // In order to remove any previously auto-generated parameters, we must
    // build the list of parameters we wish to keep, then erase all parameters
    // in the transformation, then re-add the keepers list. Yes. really.

    for ( String parameter : parametersAdded ) {

      try {
        description = trans.getParameterDescription( parameter );

        if ( description.equalsIgnoreCase( AUTO_GENERATED_PARAMETER ) ) {
          if ( variablesUsed.contains( parameter ) ) {
            params.addParameterDefinition( parameter, trans.getParameterDefault( parameter ), description );
          }
        } else {
          params.addParameterDefinition( parameter, trans.getParameterDefault( parameter ), description );
        }

      } catch ( Exception e ) {
        log.logError( "Can not locate parameter " + parameter + ".", e );
      }

    }

    trans.eraseParameters();
    for ( String key : params.listParameters() ) {
      try {
        trans.addParameterDefinition( key, params.getParameterDefault( key ), params.getParameterDescription( key ) );
      } catch ( Exception e ) {
        log.logError( "Cannot add parameter " + key + ".", e );
      }
    }
    trans.activateParameters();

  }

  public void saveMeta( MongoDbInputMeta meta ) {
    meta.setOutputJson( false );
    meta.setJsonQuery( this.jsonQuery );
    meta.setAuthenticationDatabaseName( this.authenticationDatabaseName );
    meta.setAuthenticationPassword( this.authenticationPassword );
    meta.setAuthenticationUser( this.authenticationUser );
    meta.setCollection( collection );
    meta.setConnectTimeout( this.m_connectTimeout );
    meta.setDbName( this.dbName );
    meta.setFieldsName( this.fieldsQuery );
    meta.setHostnames( this.hostname );
    meta.setPort( this.port );
    meta.setQueryIsPipeline( this.m_aggPipeline );
    meta.setReadPreference( this.m_readPreference );
    meta.setSocketTimeout( this.m_socketTimeout );
    meta.setMongoFields( MongoDocumentField.convertFromList( this.getFields() ) );
    meta.setUseKerberosAuthentication( m_kerberos );
    meta.setUseAllReplicaSetMembers( this.m_useAllReplicaSetMembers );
    meta.setReadPrefTagSets( MongoTag.convertFromList( this.tags ) );
  }

  private void initialize( MongoDbInputMeta m ) {
    setJsonQuery( m.getJsonQuery() );
    setAuthenticationDatabaseName( m.getAuthenticationDatabaseName() );
    setAuthenticationPassword( m.getAuthenticationPassword() );
    setAuthenticationUser( m.getAuthenticationUser() );
    setCollection( m.getCollection() );
    setCollections( new Vector<String>() );
    setDbName( m.getDbName() );
    setDbNames( new Vector<String>() );
    setFieldsName( m.getFieldsName() );
    setHostnames( m.getHostnames() );
    setPort( m.getPort() );
    setQueryIsPipeline( m.getQueryIsPipeline() );
    setReadPreference( m.getReadPreference() );
    setConnectTimeout( m.getConnectTimeout() );
    setSocketTimeout( m.getSocketTimeout() );
    MongoDocumentField.convertList( m.getMongoFields(), getFields() );
    setUseAllReplicaMembers( m.getUseAllReplicaSetMembers() );
    setUseKerberosAuthentication( m.getUseKerberosAuthentication() );
    MongoTag.convertList( m.getReadPrefTagSets(), getTags() );
  }

  public AbstractModelList<MongoTag> getTags() {
    return tags;
  }

  public void clear() {
    MongoDbInputMeta m = new MongoDbInputMeta();
    m.setReadPreference( NamedReadPreference.PRIMARY.getName() );
    initialize( m );
  }

  public Collection<String> getPossibleReadPreferences() {

    return NamedReadPreference.getPreferenceNames();

  }

  /**
   * Retrieve the list of database names from MongoDB based on what the user entered for hostname, port,etc.
   * NOTE: Much of this could move to the MongoDbInputData class, as it is copied almost verbatim from the
   * Spoon MongoDbInputDialog class.
   *
   * @return Vector<String> list of database names
   * @throws Exception Should anything go wrong connecting to MongoDB, it will be reported with this exception
   */
  public Vector<String> getDatabaseNamesFromMongo() throws Exception {
    Vector<String> dbs = new Vector<String>();

    if ( Const.isEmpty( hostname ) ) {
      log.logBasic( "Fetching database names aborted. Missing hostname." );
      return dbs;
    }

    final MongoDbInputMeta meta = new MongoDbInputMeta();
    final TransMeta transMeta = new TransMeta();
    saveMeta( meta );
    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
      List<String> dbNames = null;
      try {
        dbNames = wrapper.getDatabaseNames();
      } finally {
        wrapper.dispose();
      }

      for ( String s : dbNames ) {
        dbs.add( s );
      }
      return dbs;
    } catch ( Exception e ) {
      log.logError( "Unexpected error retrieving database names from MongoDb. Check your connection details.", meta );
      throw new MongoDbException(
          "Unexpected error retrieving database names from MongoDb. Check your connection details.", e );
    }
  }

  /**
   * Retrieve the list of collection names from MongoDB based on what the user entered for hostname, port,etc.
   * NOTE: Much of this could move to the MongoDbInputData class, as it is copied almost verbatim from the
   * Spoon MongoDbInputDialog class.
   *
   * @return Vector<String> list of collection names
   * @throws Exception Should anything go wrong connecting to MongoDB, it will be reported with this exception
   */
  public Vector<String> getCollectionNamesFromMongo() throws MongoDbException {
    Vector<String> newCollections = new Vector<String>();

    if ( Const.isEmpty( dbName ) || Const.isEmpty( hostname ) ) {
      log.logBasic( "Fetching collection names aborted. Missing database name or hostname." );
      return newCollections;
    }

    MongoDbInputMeta meta = new MongoDbInputMeta();
    saveMeta( meta );
    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, new TransMeta(), log );
      Set<String> collections = new HashSet<String>();
      try {
        collections = wrapper.getCollectionsNames( dbName );
      } finally {
        wrapper.dispose();
      }

      for ( String c : collections ) {
        newCollections.add( c );
      }
      return newCollections;
    } catch ( Exception e ) {
      log.logError(
          "Unexpected error retrieving collection names from MongoDb. Check that your database name is valid.", meta );
      throw new MongoDbException(
          "Unexpected error retrieving collection names from MongoDb. Check that your database name is valid.", e );
    }
  }

  /**
   * @param mergeStrategy 0 = Add new
   *                      1 = Add all
   *                      2 = Clear and add all
   *                      3 = Cancel
   * @throws MongoDbException
   */
  public void getFieldsFromMongo( int mergeStrategy ) throws MongoDbException {
    // TODO: This should be a sample dialog requested from the user ...
    int samples = 100;
    MongoDbInputMeta meta = new MongoDbInputMeta();
    if ( samples > 0 ) {
      try {

        saveMeta( meta );
        boolean result = MongoDbInputDialog.discoverFields( meta, new TransMeta(), samples );

        if ( !result ) {
          log.logBasic( "No fields were returned from MongoDb. Check your query, and/or connection details." );
          throw new MongoDbException(
              "No fields were returned from MongoDb. Check your query, and/or connection details." );
        } else {
          switch ( mergeStrategy ) {
            case 0:
              MongoDocumentField.trimList( meta.getMongoFields(), getFields() );
              break;
            case 1:
              break;
            case 2:
              getFields().removeAll( getFields() );
              break;
          }
          MongoDocumentField.convertList( meta.getMongoFields(), getFields() );
        }
      } catch ( KettleException e ) {
        log.logError( "Unexpected error retrieving fields from MongoDb. Check your connection details.", meta );
        throw new MongoDbException( "Unexpected error retrieving fields from MongoDb. Check your connection details.",
            e );
      }
    }
  }

  /**
   * @param mergeStrategy 0 = Add new
   *                      1 = Add all
   *                      2 = Clear and add all
   *                      3 = Cancel
   * @throws MongoDbException
   */
  public void getTagsFromMongo( int mergeStrategy ) throws MongoDbException {

    if ( Const.isEmpty( hostname ) ) {
      log.logBasic( "Fetching tags aborted. Missing hostname." );
      return;
    }

    MongoDbInputMeta meta = new MongoDbInputMeta();
    saveMeta( meta );

    try {
      List<String> repSetTags = new ArrayList<String>();
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, new TransMeta(), log );
      try {
        repSetTags = wrapper.getAllTags();
      } finally {
        wrapper.dispose();
      }

      switch ( mergeStrategy ) {
        case 0:
          MongoTag.trimList( repSetTags, getTags() );
          break;
        case 1:
          break;
        case 2:
          getTags().removeAll( getTags() );
          break;
      }

      MongoTag.convertList( repSetTags, getTags() );

    } catch ( Exception e ) {
      log.logError( "Unexpected error retrieving tags from MongoDb. Check connection details.", e );
      throw new MongoDbException( "Unexpected error retrieving tags from MongoDb. Check your connection details.", e );
    }
  }

  public List<String> testSelectedTags() throws MongoDbException {
    List<String> tagSets = null;

    if ( Const.isEmpty( hostname ) ) {
      log.logBasic( "Testing tags aborted. Missing hostname." );
      return tagSets;
    }

    if ( tags.isEmpty() ) {
      log.logBasic( "No tags available for testing." );
      return tagSets;
    }

    List<DBObject> mongoTagSets = new ArrayList<DBObject>();
    List<String> setsToTest = MongoTag.convertFromList( tags );

    for ( String tagSet : setsToTest ) {
      try {
        DBObject set = (DBObject) JSON.parse( tagSet );
        if ( set != null ) {
          mongoTagSets.add( set );
        }
      } catch ( Exception e ) {
        log.logError( "Error parsing MongoDb tag sets.", e );
        throw new MongoDbException( "Error parsing MongoDb tag sets. Check your tag set names and try again.", e );
      }
    }

    if ( mongoTagSets.isEmpty() ) {
      log.logBasic( "Could not parse tags for testing." );
      return tagSets;
    }

    MongoDbInputMeta meta = new MongoDbInputMeta();
    saveMeta( meta );

    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, new TransMeta(), log );
      List<String> result = new ArrayList<String>();
      try {
        result = wrapper.getReplicaSetMembersThatSatisfyTagSets( mongoTagSets );
      } finally {
        wrapper.dispose();
      }

      if ( result.size() == 0 ) {
        log.logBasic( "No replica set members match tag sets." );
        return tagSets;
      }

      tagSets = new ArrayList<String>();
      for ( String dbObject : result ) {
        tagSets.add( dbObject );
      }

    } catch ( Exception e ) {
      log.logError( "Unexpected error evaluating tag sets against replica members.", e );
      throw new MongoDbException( "Unexpected error evaluating tag sets against replica members.", e );
    }

    return tagSets;
  }
}
