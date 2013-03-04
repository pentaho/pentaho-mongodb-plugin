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

package org.pentaho.reporting.ui.datasources.mongodb.models;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.stereotype.Bindable;

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
  private String m_readPreference = "primary";
  
  private List<MongoDocumentField> fields = new ArrayList<MongoDocumentField>();

  private MongoDbInputMeta mongo;

  public MongoDbModel(MongoDbInputMeta mongo) {
    super();
    this.mongo = mongo;
    
    initialize();
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

    firePropertyChange("hostname", prevVal, hostname);
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
    
    firePropertyChange("database", prevVal, dbName);
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
    
    firePropertyChange("collection", prevVal, collection);
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
    
    firePropertyChange("m_connectTimeout", prevVal, to);
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
    
    firePropertyChange("m_socketTimeout", prevVal, so);
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
    
    firePropertyChange("m_readPreference", prevVal, preference);
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

  public List<MongoDocumentField> getFields() {
    return fields;
  }

  public void setFields(List<MongoDocumentField> fields) {
    this.fields = fields;
  }

  public void save() {
    saveMeta(mongo);
  }
  
  public void saveMeta(MongoDbInputMeta meta){
    meta.setOutputJson(false);
    meta.setJsonQuery(jsonQuery);
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

  private void initialize() {
    setJsonQuery(mongo.getJsonQuery());
    setAuthenticationPassword(mongo.getAuthenticationPassword());
    setAuthenticationUser(mongo.getAuthenticationUser());
    setCollection(mongo.getCollection());
    setCollections(new Vector<String>());
    setDbName(mongo.getDbName());
    setDbNames(new Vector<String>());
    setFieldsName(mongo.getFieldsName());
    setHostnames(mongo.getHostnames());
    setPort(mongo.getPort());
    setQueryIsPipeline(mongo.getQueryIsPipeline());
    setReadPreference(mongo.getReadPreference());
    setSocketTimeout(mongo.getSocketTimeout());
    this.setFields(MongoDocumentField.convertList(mongo.getMongoFields()));
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
      //TODO = what's on the else side here? This button should be disabled until hostname is given...
      return dbs;
    }

    MongoClient conn = null;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    saveMeta(meta);
    try {
      conn = MongoDbInputData.initConnection(meta, new TransMeta());
      List<String> dbNames = conn.getDatabaseNames();

      for (String s : dbNames) {
        dbs.add(s);
      }

    } catch (Exception e) {
      // TODO: throw new exception here, and report to dialog that an error occurred...
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
  public Vector<String> getCollectionNamesFromMongo() {
    Vector<String> newCollections = new Vector<String>();
    
    if (Const.isEmpty(dbName) || Const.isEmpty(hostname)) {
      // TODO: if the database name is empty, the button should be disabled...
      return newCollections;
    }

    MongoClient conn = null;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    saveMeta(meta);
    try {
      conn = MongoDbInputData.initConnection(meta, new TransMeta());
      DB theDB = conn.getDB(dbName);

      if (!Const.isEmpty(authenticationUser) || !Const.isEmpty(authenticationPassword)) {
        CommandResult comResult = theDB.authenticateCommand(authenticationUser,
                                                            authenticationPassword.toCharArray());
        if (!comResult.ok()) {
            // TODO: throw new exception here, and report to dialog that an error occurred...
        }
      }

      Set<String> collections = theDB.getCollectionNames();
      for (String c : collections) {
        newCollections.add(c);
      }

    } catch (Exception e) {
      // TODO: throw new exception here, and report to dialog that an error occurred...
    }finally{
      if (conn!=null){
        conn.close();
        conn = null;
      }
    }
    return newCollections;
  }
  
  public void getFieldsFromMongo(){
    // TODO: This should be a sample dialog requested from the user ...
    int samples = 100;
    MongoDbInputMeta meta = new MongoDbInputMeta(); 
    if (samples > 0) {
      try {
        saveMeta(meta);
        boolean result = MongoDbInputData.discoverFields(meta, new TransMeta(),
            samples);

        if (!result) {
            // TODO: Deal with error here ....
        } else {
          this.fields = MongoDocumentField.convertList(meta.getMongoFields());
        }
      } catch (KettleException e) {
        // TODO: log and rethrow exception so UI has a chance to deal with it...
      }
    }    
  }


}