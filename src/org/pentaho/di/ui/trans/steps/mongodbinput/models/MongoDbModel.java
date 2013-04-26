/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Copyright 2009 Pentaho Corporation.  All rights reserved.
 */

package org.pentaho.di.ui.trans.steps.mongodbinput.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.stereotype.Bindable;
import org.pentaho.ui.xul.util.AbstractModelList;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;

public class MongoDbModel extends XulEventSourceAdapter {

  private String hostname;

  private String port;

  private String dbName;
  private Vector<String> dbNames = new Vector<String>();

  private String collection;
  private Vector<String> collections = new Vector<String>();
  
  private String fieldsQuery;

  private String authenticationUser;

  private String authenticationPassword;

  private String jsonQuery;

  private boolean m_aggPipeline = false;

  private String m_connectTimeout = ""; // default - never time out

  private String m_socketTimeout = ""; // default - never time out

  /** primary, primaryPreferred, secondary, secondaryPreferred, nearest */
  private String m_readPreference = "Primary";
  
  private AbstractModelList<MongoDocumentField> fields = new AbstractModelList<MongoDocumentField>();

  private MongoDbInputMeta mongo;
  
  private LogChannel log; 

  public MongoDbModel(MongoDbInputMeta mongo) {
    super();
    this.mongo = mongo;
    log = new LogChannel(this.mongo);
    initialize(this.mongo);
  }
  
  public boolean validate(){
    boolean valid = false; 
    
    valid = (!StringUtil.isEmpty(hostname)) &&
         //   (!StringUtil.isEmpty(port)) &&     // port can be empty; MongoDb will assume 27017
            (!StringUtil.isEmpty(dbName)) && 
            (!StringUtil.isEmpty(collection)) && 
            (fields.size() > 0);
    
    firePropertyChange("validate", null, valid);
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
  public void setHostnames(String hostname) {
    String prevVal = this.hostname;
    this.hostname = hostname;

    firePropertyChange("hostnames", prevVal, hostname);
    validate();
  }
  
  /**
   * @return the port. This is a port to use for all hostnames (avoids having to
   *         specify the same port for each hostname in the hostnames list
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port. This is a port to use for all hostnames (avoids
   *          having to specify the same port for each hostname in the hostnames
   *          list
   */
  public void setPort(String port) {
    String prevVal = this.port;
    this.port = port;
    
    firePropertyChange("port", prevVal, port);
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
  public void setDbName(String dbName) {
    String prevVal = this.dbName;
    this.dbName = dbName;
    
    firePropertyChange("database", prevVal, dbName==null?"":dbName);
  }

  public Collection<String> getDbNames() {
    return dbNames;
  }

  public void setDbNames( Vector<String> dbs ) {
    Collection <String> prevVal = this.dbNames;
    this.dbNames = dbs;
    
    // add the current selection, even if not in the list...
    // users are allowed to manually add database names
    // TODO: check behavior in Spoon...
    
    if (!Const.isEmpty(dbName)){
      if (!dbNames.contains(dbName)){
        dbNames.add(dbName);
      }
    }

    firePropertyChange("database", prevVal, dbs);
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fieldsQuery;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setFieldsName(String fields) {
    String prevVal = this.fieldsQuery;
    this.fieldsQuery = fields;
    
    firePropertyChange("fieldsQuery", prevVal, fields);
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
  public void setCollection(String collection) {
    String prevVal = this.collection;
    this.collection = collection;
    
    firePropertyChange("collection", prevVal, collection==null?"":collection);
  }

  public Collection<String> getCollections() {
    return collections;
  }

  public void setCollections( Vector<String> collections ) {
    Collection <String> prevVal = this.collections;
    this.collections = collections;

    // add the current selection, even if not in the list...
    // users are allowed to manually add collection names
    // TODO: check behavior in Spoon...
    
    if (!Const.isEmpty(collection)){
      if (!collections.contains(collection)){
        collections.add(collection);
      }
    }

    firePropertyChange("collection", prevVal, collections);
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
  public void setAuthenticationUser(String authenticationUser) {
    String prevVal = this.authenticationUser;
    this.authenticationUser = authenticationUser;
    
    firePropertyChange("authenticationUser", prevVal, authenticationUser);
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
  public void setAuthenticationPassword(String authenticationPassword) {
    String prevVal = this.authenticationPassword;
    this.authenticationPassword = authenticationPassword;
    
    firePropertyChange("authenticationPassword", prevVal, authenticationPassword);
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
  @Bindable
  public void setJsonQuery(String jsonQuery) {
    String prevVal = this.jsonQuery;
    this.jsonQuery = jsonQuery;

    firePropertyChange("jsonQuery", prevVal, jsonQuery);
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   * 
   * @param q true if the supplied query is a pipeline specification
  m_aggPipeline = q;
   */
  public void setQueryIsPipeline(boolean q) {
    Boolean prevVal = new Boolean(this.m_aggPipeline);
    m_aggPipeline = q;
    
    firePropertyChange("m_aggPipeline", prevVal, new Boolean(q));
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
   * Set the connection timeout. The default is never timeout
   * 
   * @param to the connection timeout in milliseconds
   */
  public void setConnectTimeout(String to) {
    String prevVal = this.m_connectTimeout;
    m_connectTimeout = to;
    
    firePropertyChange("connectTimeout", prevVal, to);
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
  public void setSocketTimeout(String so) {
    String prevVal = this.m_socketTimeout;
    m_socketTimeout = so;
    
    firePropertyChange("socketTimeout", prevVal, so);
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
  public void setReadPreference(String preference) {
    String prevVal = this.m_readPreference;
    m_readPreference = Const.isEmpty(preference) ? "primary": preference;
    
    firePropertyChange("readPreference", prevVal, preference);
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
    saveMeta(mongo);
  }
  
  public void saveMeta(MongoDbInputMeta meta){
    meta.setOutputJson(false);
    meta.setJsonQuery(this.jsonQuery);
    meta.setAuthenticationPassword(this.authenticationPassword);
    meta.setAuthenticationUser(this.authenticationUser);
    meta.setCollection(collection);
    meta.setConnectTimeout(this.m_connectTimeout);
    meta.setDbName(this.dbName);
    meta.setFieldsName(this.fieldsQuery);
    meta.setHostnames(this.hostname);
    meta.setPort(this.port);
    meta.setQueryIsPipeline(this.m_aggPipeline);
    meta.setReadPreference(this.m_readPreference);
    meta.setSocketTimeout(this.m_socketTimeout);
    meta.setMongoFields(MongoDocumentField.convertFromList(this.getFields()));
  }

  private void initialize(MongoDbInputMeta m) {
    setJsonQuery(m.getJsonQuery());
    setAuthenticationPassword(m.getAuthenticationPassword());
    setAuthenticationUser(m.getAuthenticationUser());
    setCollection(m.getCollection());
    setCollections(new Vector<String>());
    setDbName(m.getDbName());
    setDbNames(new Vector<String>());
    setFieldsName(m.getFieldsName());
    setHostnames(m.getHostnames());
    setPort(m.getPort());
    setQueryIsPipeline(m.getQueryIsPipeline());
    setReadPreference(m.getReadPreference());
    setConnectTimeout(m.getConnectTimeout());
    setSocketTimeout(m.getSocketTimeout());
    MongoDocumentField.convertList(m.getMongoFields(), getFields());
  }
  
  public void clear()
  {
    MongoDbInputMeta m = new MongoDbInputMeta();
    m.setReadPreference("Primary");
    initialize(m);
  }

  public Collection<String> getPossibleReadPreferences(){
    ArrayList<String> prefs = new ArrayList<String>();
    prefs.add("Primary");
    prefs.add("Primary preferred");
    prefs.add("Secondary");
    prefs.add("Secondary preferred");
    prefs.add("Nearest");
    return prefs;
  }
  
  /**
   * Retrieve the list of database names from MongoDB based on what the user entered for hostname, port,etc. 
   * NOTE: Much of this could move to the MongoDbInputData class, as it is copied almost verbatim from the 
   * Spoon MongoDbInputDialog class. 
   * @return Vector<String> list of database names
   * @throws Exception Should anything go wrong connecting to MongoDB, it will be reported with this exception
   */
  public Vector<String> getDatabaseNamesFromMongo() throws Exception {
    Vector<String> dbs = new Vector<String>();
    
    if (Const.isEmpty(hostname)) {
      log.logBasic("Fetching database names aborted. Missing hostname.");
      return dbs;
    }

    MongoClient conn = null;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    saveMeta(meta);
    try {
      conn = MongoDbInputData.initConnection(meta, new TransMeta(), null);
      List<String> dbNames = conn.getDatabaseNames();

      for (String s : dbNames) {
        dbs.add(s);
      }

    } catch (Exception e) {
      log.logError("Unexpected error retrieving database names from MongoDb. Check your connection details.", meta);
      throw new MongoDbException("Unexpected error retrieving database names from MongoDb. Check your connection details.", e);
    }finally{
      if (conn != null){
        conn.close();
        conn = null;
      }
    }
    return dbs;
  }

  /**
   * Retrieve the list of collection names from MongoDB based on what the user entered for hostname, port,etc. 
   * NOTE: Much of this could move to the MongoDbInputData class, as it is copied almost verbatim from the 
   * Spoon MongoDbInputDialog class. 
   * @return Vector<String> list of collection names
   * @throws Exception Should anything go wrong connecting to MongoDB, it will be reported with this exception
   */
  public Vector<String> getCollectionNamesFromMongo() throws MongoDbException{
    Vector<String> newCollections = new Vector<String>();
    
    if (Const.isEmpty(dbName) || Const.isEmpty(hostname)) {
      log.logBasic("Fetching collection names aborted. Missing database name or hostname.");
      return newCollections;
    }

    MongoClient conn = null;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    saveMeta(meta);
    try {
      conn = MongoDbInputData.initConnection(meta, new TransMeta(), null);
      DB theDB = conn.getDB(dbName);

      if (!Const.isEmpty(authenticationUser) || !Const.isEmpty(authenticationPassword)) {
        CommandResult comResult = theDB.authenticateCommand(authenticationUser,
                                                            authenticationPassword.toCharArray());
        if (!comResult.ok()) {
          log.logBasic("Failed to authenticate user {0}.", authenticationUser);
          throw new MongoDbException("Failed to autheticate the user. Check your credentials.");
        }
      }

      Set<String> collections = theDB.getCollectionNames();
      for (String c : collections) {
        newCollections.add(c);
      }

    } catch (Exception e) {
      log.logError("Unexpected error retrieving collection names from MongoDb. Check that your database name is valid.", meta);
      throw new MongoDbException("Unexpected error retrieving collection names from MongoDb. Check that your database name is valid.", e);
    }finally{
      if (conn!=null){
        conn.close();
        conn = null;
      }
    }
    return newCollections;
  }
  
  public void getFieldsFromMongo() throws MongoDbException{
    // TODO: This should be a sample dialog requested from the user ...
    int samples = 100;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    if (samples > 0) {
      try {

        saveMeta(meta);
        boolean result = MongoDbInputData.discoverFields(meta, new TransMeta(),
            samples);

        if (!result) {
          log.logBasic("No fields were returned from MongoDb. Check your query, and/or connection details.");
          throw new MongoDbException("No fields were returned from MongoDb. Check your query, and/or connection details.");
        } else {
          MongoDocumentField.convertList(meta.getMongoFields(), getFields());
        }
      } catch (KettleException e) {
       log.logError("Unexpected error retrieving fields from MongoDb. Check your connection details.", meta);
       throw new MongoDbException("Unexpected error retrieving fields from MongoDb. Check your connection details.", e);
      }
    }    
  }


}