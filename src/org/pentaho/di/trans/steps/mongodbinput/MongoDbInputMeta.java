/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.mongodbinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

/**
 * Created on 8-apr-2011
 * 
 * @author matt
 * @since 4.2.0-M1
 */
@Step(id = "MongoDbInput", image = "mongodb-input.png", name = "MongoDB Input", description = "Reads from a Mongo DB collection", categoryDescription = "Big Data")
public class MongoDbInputMeta extends BaseStepMeta implements StepMetaInterface {
  protected static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes

  private String hostname;
  private String port;
  private String dbName;
  private String collection;
  private String jsonFieldName;
  private String fields;

  private String authenticationUser;
  private String authenticationPassword;

  private String jsonQuery;

  private boolean m_aggPipeline = false;

  private boolean m_outputJson = true;

  private String m_connectTimeout = ""; // default - never time out //$NON-NLS-1$

  private String m_socketTimeout = ""; // default - never time out //$NON-NLS-1$

  /** primary, primaryPreferred, secondary, secondaryPreferred, nearest */
  private String m_readPreference = "primary"; //$NON-NLS-1$

  /**
   * whether to discover and use all replica set members (if not already
   * specified in the hosts field)
   */
  private boolean m_useAllReplicaSetMembers;

  private List<MongoDbInputData.MongoField> m_fields;

  /** optional tag sets to use with read preference settings */
  private List<String> m_readPrefTagSets;

  private boolean m_executeForEachIncomingRow = false;

  public MongoDbInputMeta() {
    super(); // allocate BaseStepMeta
  }

  public void setMongoFields(List<MongoDbInputData.MongoField> fields) {
    m_fields = fields;
  }

  public List<MongoDbInputData.MongoField> getMongoFields() {
    return m_fields;
  }

  public void setReadPrefTagSets(List<String> tagSets) {
    m_readPrefTagSets = tagSets;
  }

  public List<String> getReadPrefTagSets() {
    return m_readPrefTagSets;
  }

  public void setUseAllReplicaSetMembers(boolean u) {
    m_useAllReplicaSetMembers = u;
  }

  public boolean getUseAllReplicaSetMembers() {
    return m_useAllReplicaSetMembers;
  }

  public void setExecuteForEachIncomingRow(boolean e) {
    m_executeForEachIncomingRow = e;
  }

  public boolean getExecuteForEachIncomingRow() {
    return m_executeForEachIncomingRow;
  }

  @Override
  public void loadXML(Node stepnode, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleXMLException {
    readData(stepnode);
  }

  @Override
  public Object clone() {
    MongoDbInputMeta retval = (MongoDbInputMeta) super.clone();
    return retval;
  }

  private void readData(Node stepnode) throws KettleXMLException {
    try {
      hostname = XMLHandler.getTagValue(stepnode, "hostname"); //$NON-NLS-1$ 
      port = XMLHandler.getTagValue(stepnode, "port"); //$NON-NLS-1$ 
      dbName = XMLHandler.getTagValue(stepnode, "db_name"); //$NON-NLS-1$
      fields = XMLHandler.getTagValue(stepnode, "fields_name"); //$NON-NLS-1$
      collection = XMLHandler.getTagValue(stepnode, "collection"); //$NON-NLS-1$
      jsonFieldName = XMLHandler.getTagValue(stepnode, "json_field_name"); //$NON-NLS-1$
      jsonQuery = XMLHandler.getTagValue(stepnode, "json_query"); //$NON-NLS-1$
      authenticationUser = XMLHandler.getTagValue(stepnode, "auth_user"); //$NON-NLS-1$
      authenticationPassword = Encr
          .decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue(stepnode,
              "auth_password")); //$NON-NLS-1$

      m_connectTimeout = XMLHandler.getTagValue(stepnode, "connect_timeout"); //$NON-NLS-1$
      m_socketTimeout = XMLHandler.getTagValue(stepnode, "socket_timeout"); //$NON-NLS-1$
      m_readPreference = XMLHandler.getTagValue(stepnode, "read_preference"); //$NON-NLS-1$

      m_outputJson = true; // default to true for backwards compatibility
      String outputJson = XMLHandler.getTagValue(stepnode, "output_json"); //$NON-NLS-1$
      if (!Const.isEmpty(outputJson)) {
        m_outputJson = outputJson.equalsIgnoreCase("Y"); //$NON-NLS-1$
      }

      m_useAllReplicaSetMembers = false; // default to false for backwards
                                         // compatibility
      String useAll = XMLHandler.getTagValue(stepnode,
          "use_all_replica_members"); //$NON-NLS-1$
      if (!Const.isEmpty(useAll)) {
        m_useAllReplicaSetMembers = useAll.equalsIgnoreCase("Y"); //$NON-NLS-1$
      }

      String queryIsPipe = XMLHandler
          .getTagValue(stepnode, "query_is_pipeline"); //$NON-NLS-1$
      if (!Const.isEmpty(queryIsPipe)) {
        m_aggPipeline = queryIsPipe.equalsIgnoreCase("Y"); //$NON-NLS-1$
      }

      String executeForEachR = XMLHandler.getTagValue(stepnode,
          "execute_for_each_row");
      if (!Const.isEmpty(executeForEachR)) {
        m_executeForEachIncomingRow = executeForEachR.equalsIgnoreCase("Y");
      }

      Node mongo_fields = XMLHandler.getSubNode(stepnode, "mongo_fields"); //$NON-NLS-1$
      if (mongo_fields != null
          && XMLHandler.countNodes(mongo_fields, "mongo_field") > 0) { //$NON-NLS-1$
        int nrfields = XMLHandler.countNodes(mongo_fields, "mongo_field"); //$NON-NLS-1$

        m_fields = new ArrayList<MongoDbInputData.MongoField>();
        for (int i = 0; i < nrfields; i++) {
          Node fieldNode = XMLHandler.getSubNodeByNr(mongo_fields,
              "mongo_field", i); //$NON-NLS-1$

          MongoDbInputData.MongoField newField = new MongoDbInputData.MongoField();
          newField.m_fieldName = XMLHandler
              .getTagValue(fieldNode, "field_name"); //$NON-NLS-1$
          newField.m_fieldPath = XMLHandler
              .getTagValue(fieldNode, "field_path"); //$NON-NLS-1$
          newField.m_kettleType = XMLHandler.getTagValue(fieldNode,
              "field_type"); //$NON-NLS-1$
          String indexedVals = XMLHandler
              .getTagValue(fieldNode, "indexed_vals"); //$NON-NLS-1$
          if (indexedVals != null && indexedVals.length() > 0) {
            newField.m_indexedVals = MongoDbInputData
                .indexedValsList(indexedVals);
          }

          m_fields.add(newField);
        }
      }

      String tags = XMLHandler.getTagValue(stepnode, "tag_sets"); //$NON-NLS-1$
      if (!Const.isEmpty(tags)) {
        m_readPrefTagSets = new ArrayList<String>();

        String[] parts = tags.split("#@#"); //$NON-NLS-1$
        for (String p : parts) {
          m_readPrefTagSets.add(p.trim());
        }
      }
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnableToLoadStepInfo"), e); //$NON-NLS-1$
    }
  }

  public void setDefault() {
    hostname = "localhost"; //$NON-NLS-1$
    port = "27017"; //$NON-NLS-1$
    dbName = "db"; //$NON-NLS-1$
    collection = "collection"; //$NON-NLS-1$
    jsonFieldName = "json"; //$NON-NLS-1$
  }

  @SuppressWarnings("deprecation")
  @Override
  public void getFields(RowMetaInterface rowMeta, String origin,
      RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
      throws KettleStepException {

    if (m_outputJson || m_fields == null || m_fields.size() == 0) {
      ValueMetaInterface jsonValueMeta = new ValueMeta(jsonFieldName,
          ValueMetaInterface.TYPE_STRING);
      jsonValueMeta.setOrigin(origin);
      rowMeta.addValueMeta(jsonValueMeta);
    } else {
      for (MongoDbInputData.MongoField f : m_fields) {
        ValueMetaInterface vm = new ValueMeta();
        vm.setName(f.m_fieldName);
        vm.setOrigin(origin);
        vm.setType(ValueMeta.getType(f.m_kettleType));
        if (f.m_indexedVals != null) {
          vm.setIndex(f.m_indexedVals.toArray()); // indexed values
        }
        rowMeta.addValueMeta(vm);
      }
    }
  }

  protected String tagSetsToString() {
    if (m_readPrefTagSets != null && m_readPrefTagSets.size() > 0) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < m_readPrefTagSets.size(); i++) {
        String s = m_readPrefTagSets.get(i);
        s = s.trim();
        if (!s.startsWith("{")) { //$NON-NLS-1$
          s = "{" + s; //$NON-NLS-1$
        }
        if (!s.endsWith("}")) { //$NON-NLS-1$
          s += "}"; //$NON-NLS-1$
        }

        builder.append(s);
        if (i != m_readPrefTagSets.size() - 1) {
          builder.append(s).append("#@#"); //$NON-NLS-1$
        }
      }
      return builder.toString();
    }
    return null;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);

    retval.append("    ").append(XMLHandler.addTagValue("hostname", hostname)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("port", port)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("use_all_replica_members", m_useAllReplicaSetMembers)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("db_name", dbName)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("fields_name", fields)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("collection", collection)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("json_field_name", jsonFieldName)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("json_query", jsonQuery)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("auth_user", authenticationUser)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("auth_password", //$NON-NLS-1$
            Encr.encryptPasswordIfNotUsingVariables(authenticationPassword)));
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("connect_timeout", m_connectTimeout)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("socket_timeout", m_socketTimeout)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("read_preference", m_readPreference)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("output_json", m_outputJson)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue("query_is_pipeline", m_aggPipeline)); //$NON-NLS-1$
    retval.append("    ").append( //$NON-NLS-1$
        XMLHandler.addTagValue(
            "execute_for_each_row", m_executeForEachIncomingRow)); //$NON-NLS-1$

    if (m_fields != null && m_fields.size() > 0) {
      retval.append("\n    ").append(XMLHandler.openTag("mongo_fields")); //$NON-NLS-1$ //$NON-NLS-2$

      for (MongoDbInputData.MongoField f : m_fields) {
        retval.append("\n      ").append(XMLHandler.openTag("mongo_field")); //$NON-NLS-1$ //$NON-NLS-2$

        retval.append("\n        ").append( //$NON-NLS-1$
            XMLHandler.addTagValue("field_name", f.m_fieldName)); //$NON-NLS-1$
        retval.append("\n        ").append( //$NON-NLS-1$
            XMLHandler.addTagValue("field_path", f.m_fieldPath)); //$NON-NLS-1$
        retval.append("\n        ").append( //$NON-NLS-1$
            XMLHandler.addTagValue("field_type", f.m_kettleType)); //$NON-NLS-1$
        if (f.m_indexedVals != null && f.m_indexedVals.size() > 0) {
          retval.append("\n        ").append( //$NON-NLS-1$
              XMLHandler.addTagValue("indexed_vals", //$NON-NLS-1$
                  MongoDbInputData.indexedValsList(f.m_indexedVals)));
        }
        retval.append("\n      ").append(XMLHandler.closeTag("mongo_field")); //$NON-NLS-1$ //$NON-NLS-2$
      }

      retval.append("\n    ").append(XMLHandler.closeTag("mongo_fields")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    String tags = tagSetsToString();
    if (!Const.isEmpty(tags)) {
      retval.append("    ").append(XMLHandler.addTagValue("tag_sets", tags)); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return retval.toString();
  }

  @Override
  public void readRep(Repository rep, ObjectId id_step,
      List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleException {
    try {
      hostname = rep.getStepAttributeString(id_step, "hostname"); //$NON-NLS-1$
      port = rep.getStepAttributeString(id_step, "port"); //$NON-NLS-1$
      m_useAllReplicaSetMembers = rep.getStepAttributeBoolean(id_step, 0,
          "use_all_replica_members"); //$NON-NLS-1$
      dbName = rep.getStepAttributeString(id_step, "db_name"); //$NON-NLS-1$
      fields = rep.getStepAttributeString(id_step, "fields_name"); //$NON-NLS-1$
      collection = rep.getStepAttributeString(id_step, "collection"); //$NON-NLS-1$
      jsonFieldName = rep.getStepAttributeString(id_step, "json_field_name"); //$NON-NLS-1$
      jsonQuery = rep.getStepAttributeString(id_step, "json_query"); //$NON-NLS-1$

      authenticationUser = rep.getStepAttributeString(id_step, "auth_user"); //$NON-NLS-1$
      authenticationPassword = Encr.decryptPasswordOptionallyEncrypted(rep
          .getStepAttributeString(id_step, "auth_password")); //$NON-NLS-1$
      m_connectTimeout = rep.getStepAttributeString(id_step, "connect_timeout"); //$NON-NLS-1$
      m_socketTimeout = rep.getStepAttributeString(id_step, "socket_timeout"); //$NON-NLS-1$
      m_readPreference = rep.getStepAttributeString(id_step, "read_preference"); //$NON-NLS-1$

      m_outputJson = rep.getStepAttributeBoolean(id_step, 0, "output_json"); //$NON-NLS-1$
      m_aggPipeline = rep.getStepAttributeBoolean(id_step, "query_is_pipeline"); //$NON-NLS-1$
      m_executeForEachIncomingRow = rep.getStepAttributeBoolean(id_step,
          "execute_for_each_row"); //$NON-NLS-1$

      int nrfields = rep.countNrStepAttributes(id_step, "field_name"); //$NON-NLS-1$
      if (nrfields > 0) {
        m_fields = new ArrayList<MongoDbInputData.MongoField>();

        for (int i = 0; i < nrfields; i++) {
          MongoDbInputData.MongoField newField = new MongoDbInputData.MongoField();

          newField.m_fieldName = rep.getStepAttributeString(id_step, i,
              "field_name"); //$NON-NLS-1$
          newField.m_fieldPath = rep.getStepAttributeString(id_step, i,
              "field_path"); //$NON-NLS-1$
          newField.m_kettleType = rep.getStepAttributeString(id_step, i,
              "field_type"); //$NON-NLS-1$
          String indexedVals = rep.getStepAttributeString(id_step, i,
              "indexed_vals"); //$NON-NLS-1$
          if (indexedVals != null && indexedVals.length() > 0) {
            newField.m_indexedVals = MongoDbInputData
                .indexedValsList(indexedVals);
          }

          m_fields.add(newField);
        }
      }

      String tags = rep.getStepAttributeString(id_step, "tag_sets"); //$NON-NLS-1$
      if (!Const.isEmpty(tags)) {
        m_readPrefTagSets = new ArrayList<String>();

        String[] parts = tags.split("?@?"); //$NON-NLS-1$
        for (String p : parts) {
          m_readPrefTagSets.add(p.trim());
        }
      }
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e); //$NON-NLS-1$
    }
  }

  @Override
  public void saveRep(Repository rep, ObjectId id_transformation,
      ObjectId id_step) throws KettleException {
    try {
      rep.saveStepAttribute(id_transformation, id_step, "hostname", hostname); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "port", port); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "use_all_replica_members", m_useAllReplicaSetMembers); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "db_name", dbName); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "fields_name", fields); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "collection", collection); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "json_field_name", jsonFieldName); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "json_query", jsonQuery); //$NON-NLS-1$

      rep.saveStepAttribute(id_transformation, id_step, "auth_user", //$NON-NLS-1$
          authenticationUser);
      rep.saveStepAttribute(id_transformation, id_step, "auth_password", //$NON-NLS-1$
          Encr.encryptPasswordIfNotUsingVariables(authenticationPassword));
      rep.saveStepAttribute(id_transformation, id_step,
          "connect_timeout", m_connectTimeout); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "socket_timeout", m_socketTimeout); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "read_preference", m_readPreference); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, 0, "output_json", //$NON-NLS-1$
          m_outputJson);
      rep.saveStepAttribute(id_transformation, id_step, 0, "query_is_pipeline", //$NON-NLS-1$
          m_aggPipeline);
      rep.saveStepAttribute(id_transformation, id_step, 0,
          "execute_for_each_row", //$NON-NLS-1$
          m_executeForEachIncomingRow);

      if (m_fields != null && m_fields.size() > 0) {
        for (int i = 0; i < m_fields.size(); i++) {
          MongoDbInputData.MongoField f = m_fields.get(i);

          rep.saveStepAttribute(id_transformation, id_step, i, "field_name", //$NON-NLS-1$
              f.m_fieldName);
          rep.saveStepAttribute(id_transformation, id_step, i, "field_path", //$NON-NLS-1$
              f.m_fieldPath);
          rep.saveStepAttribute(id_transformation, id_step, i, "field_type", //$NON-NLS-1$
              f.m_kettleType);
          if (f.m_indexedVals != null && f.m_indexedVals.size() > 0) {
            String indexedVals = MongoDbInputData
                .indexedValsList(f.m_indexedVals);

            rep.saveStepAttribute(id_transformation, id_step, i,
                "indexed_vals", indexedVals); //$NON-NLS-1$
          }
        }
      }

      String tags = tagSetsToString();
      if (!Const.isEmpty(tags)) {
        rep.saveStepAttribute(id_transformation, id_step, "tag_sets", tags); //$NON-NLS-1$
      }
    } catch (KettleException e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnableToSaveStepInfo") + id_step, e); //$NON-NLS-1$
    }
  }

  public StepInterface getStep(StepMeta stepMeta,
      StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans) {
    return new MongoDbInput(stepMeta, stepDataInterface, cnr, tr, trans);
  }

  public StepDataInterface getStepData() {
    return new MongoDbInputData();
  }

  @Override
  public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
      StepMeta stepMeta, RowMetaInterface prev, String[] input,
      String[] output, RowMetaInterface info) {
    // TODO add checks
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
    this.hostname = hostname;
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
    this.port = port;
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
    this.dbName = dbName;
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fields;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setFieldsName(String fields) {
    this.fields = fields;
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
    this.collection = collection;
  }

  /**
   * @return the jsonFieldName
   */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /**
   * @param jsonFieldName the jsonFieldName to set
   */
  public void setJsonFieldName(String jsonFieldName) {
    this.jsonFieldName = jsonFieldName;
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
    this.authenticationUser = authenticationUser;
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
    this.authenticationPassword = authenticationPassword;
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
  public void setJsonQuery(String jsonQuery) {
    this.jsonQuery = jsonQuery;
  }

  /**
   * Set whether to output just a single field as JSON
   * 
   * @param outputJson true if a single field containing JSON is to be output
   */
  public void setOutputJson(boolean outputJson) {
    m_outputJson = outputJson;
  }

  /**
   * Get whether to output just a single field as JSON
   * 
   * @return true if a single field containing JSON is to be output
   */
  public boolean getOutputJson() {
    return m_outputJson;
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   * 
   * @param q true if the supplied query is a pipeline specification
   */
  public void setQueryIsPipeline(boolean q) {
    m_aggPipeline = q;
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
    m_connectTimeout = to;
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
    m_socketTimeout = so;
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
    m_readPreference = preference;
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
}
