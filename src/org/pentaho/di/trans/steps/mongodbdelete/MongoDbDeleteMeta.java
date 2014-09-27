/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.di.trans.steps.mongodbdelete;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * Meta data class for MongoDbDelete step.
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
@Step(id = "MongoDbDelete", image = "mongodb-delete.png", name = "MongoDB Delete",
        description = "Delete document inside a Mongo DB collection", categoryDescription = "Big Data")
public class MongoDbDeleteMeta extends MongoDbMeta implements StepMetaInterface {

    private static Class<?> PKG = MongoDbDeleteMeta.class;
    public static final int RETRIES = 5;
    public static final int RETRY_DELAY = 10; // seconds
    private String m_writeRetries = "" + RETRIES;
    private String m_writeRetryDelay = "" + RETRY_DELAY;
    /** The list of paths to document fields for incoming kettle values */
    protected List<MongoDbDeleteMeta.MongoField> m_mongoFields;

    @Override
    public void setDefault() {
        setHostnames("localhost");
        setPort("27017");
        setCollection("");
        setDbName("");
        setWriteConcern("");
        setWTimeout("");
        setJournal(false);
    }

    @Override
    public StepInterface getStep(StepMeta sm, StepDataInterface sdi, int i, TransMeta tm, Trans trans) {
        return new MongoDbDelete(sm, sdi, i, tm, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new MongoDbDeleteData();
    }

    @Override
    public String getDialogClassName() {
        return "org.pentaho.di.trans.steps.mongodbdelete.MongoDbDeleteDialog";
    }

    public static class MongoField {

        public String m_incomingField1 = "";
        public String m_incomingField2 = "";
        public String m_mongoDocPath = "";
        public String m_comparator = "";
        protected List<String> m_pathList;
        protected List<String> m_tempPathList;

        public MongoField copy() {
            MongoField newF = new MongoField();
            newF.m_incomingField1 = m_incomingField1;
            newF.m_incomingField2 = m_incomingField2;
            newF.m_mongoDocPath = m_mongoDocPath;
            newF.m_comparator = m_comparator;

            return newF;
        }

        public void init(VariableSpace vars) {
            m_pathList = new ArrayList<String>();

            String path = vars.environmentSubstitute(m_mongoDocPath);
            m_pathList.add(path);

            m_tempPathList = new ArrayList<String>(m_pathList);
        }

        public void reset() {
            if (m_tempPathList != null && m_tempPathList.size() > 0) {
                m_tempPathList.clear();
            }
            if (m_tempPathList != null) {
                m_tempPathList.addAll(m_pathList);
            }
        }
    }

    public void setMongoFields(List<MongoField> mongoFields) {
        m_mongoFields = mongoFields;
    }

    public List<MongoField> getMongoFields() {
        return m_mongoFields;
    }

    /**
     * Sets write retries.
     *
     * @param r the number of retry attempts to make
     */
    public void setWriteRetries(String r) {
        m_writeRetries = r;
    }

    /**
     * Get the number of retry attempts to make if a particular write operation fails
     *
     * @return the number of retry attempts to make
     */
    public String getWriteRetries() {
        return m_writeRetries;
    }

    /**
     * Set the delay (in seconds) between write retry attempts
     *
     * @param d the delay in seconds between retry attempts
     */
    public void setWriteRetryDelay(String d) {
        m_writeRetryDelay = d;
    }

    /**
     * Get the delay (in seconds) between write retry attempts
     *
     * @return the delay in seconds between retry attempts
     */
    public String getWriteRetryDelay() {
        return m_writeRetryDelay;
    }

    @Override
    public String getXML() {
        StringBuffer retval = new StringBuffer();

        if (!Const.isEmpty(getHostnames())) {
            retval.append("\n    ").append(
                    XMLHandler.addTagValue("mongo_host", getHostnames()));
        }
        if (!Const.isEmpty(getPort())) {
            retval.append("\n    ").append( //$NON-NLS-1$
                    XMLHandler.addTagValue("mongo_port", getPort())); //$NON-NLS-1$
        }

        retval.append("    ").append(XMLHandler.addTagValue("use_all_replica_members", getUseAllReplicaSetMembers()));

        if (!Const.isEmpty(getAuthenticationUser())) {
            retval.append("\n    ").append(
                    XMLHandler.addTagValue("mongo_user", getAuthenticationUser()));
        }
        if (!Const.isEmpty(getAuthenticationPassword())) {
            retval.append("\n    ").append(
                    XMLHandler.addTagValue("mongo_password",
                    Encr.encryptPasswordIfNotUsingVariables(getAuthenticationPassword())));
        }

        retval.append("    ").append(
                XMLHandler.addTagValue("auth_kerberos", getUseKerberosAuthentication()));

        if (!Const.isEmpty(getDbName())) {
            retval.append("\n    ").append(
                    XMLHandler.addTagValue("mongo_db", getDbName()));
        }
        if (!Const.isEmpty(getCollection())) {
            retval.append("\n    ").append(
                    XMLHandler.addTagValue("mongo_collection", getCollection()));
        }

        retval.append("    ").append(
                XMLHandler.addTagValue("connect_timeout", getConnectTimeout()));
        retval.append("    ").append(
                XMLHandler.addTagValue("socket_timeout", getSocketTimeout()));
        retval.append("    ").append(
                XMLHandler.addTagValue("read_preference", getReadPreference()));
        retval.append("    ").append(
                XMLHandler.addTagValue("write_concern", getWriteConcern()));
        retval.append("    ").append(
                XMLHandler.addTagValue("w_timeout", getWTimeout()));
        retval.append("    ").append(
                XMLHandler.addTagValue("journaled_writes", getJournal()));

        retval.append("    ").append(
                XMLHandler.addTagValue("write_retries", m_writeRetries));
        retval.append("    ").append(
                XMLHandler.addTagValue("write_retry_delay", m_writeRetryDelay));

        if (m_mongoFields != null && m_mongoFields.size() > 0) {
            retval.append("\n    ").append(XMLHandler.openTag("mongo_fields"));

            for (MongoField field : m_mongoFields) {
                retval.append("\n      ").append(XMLHandler.openTag("mongo_field"));

                retval.append("\n         ").append(
                        XMLHandler.addTagValue("mongo_doc_path", field.m_mongoDocPath));
                retval.append("\n         ").append(
                        XMLHandler.addTagValue("comparator", field.m_comparator));
                retval.append("\n         ").append(
                        XMLHandler.addTagValue("incoming_field_1", field.m_incomingField1));
                retval.append("\n         ").append(
                        XMLHandler.addTagValue("incoming_field_2", field.m_incomingField2));

                retval.append("\n      ").append(XMLHandler.closeTag("mongo_field"));
            }

            retval.append("\n    ").append(XMLHandler.closeTag("mongo_fields"));
        }

        return retval.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        setHostnames(XMLHandler.getTagValue(stepnode, "mongo_host"));
        setPort(XMLHandler.getTagValue(stepnode, "mongo_port"));
        setAuthenticationUser(XMLHandler.getTagValue(stepnode, "mongo_user"));
        setAuthenticationPassword(XMLHandler.getTagValue(stepnode, "mongo_password"));
        if (!Const.isEmpty(getAuthenticationPassword())) {
            setAuthenticationPassword(Encr.decryptPasswordOptionallyEncrypted(getAuthenticationPassword()));
        }

        setUseKerberosAuthentication("Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "auth_kerberos")));
        setDbName(XMLHandler.getTagValue(stepnode, "mongo_db"));
        setCollection(XMLHandler.getTagValue(stepnode, "mongo_collection"));

        setConnectTimeout(XMLHandler.getTagValue(stepnode, "connect_timeout"));
        setSocketTimeout(XMLHandler.getTagValue(stepnode, "socket_timeout"));
        setReadPreference(XMLHandler.getTagValue(stepnode, "read_preference"));
        setWriteConcern(XMLHandler.getTagValue(stepnode, "write_concern"));
        setWTimeout(XMLHandler.getTagValue(stepnode, "w_timeout"));
        String journaled = XMLHandler.getTagValue(stepnode, "journaled_writes");
        if (!Const.isEmpty(journaled)) {
            setJournal(journaled.equalsIgnoreCase("Y"));
        }

        setUseAllReplicaSetMembers("Y".equalsIgnoreCase(XMLHandler.getTagValue(stepnode, "use_all_replica_members")));

        String writeRetries = XMLHandler.getTagValue(stepnode, "write_retries");
        if (!Const.isEmpty(writeRetries)) {
            m_writeRetries = writeRetries;
        }
        String writeRetryDelay = XMLHandler.getTagValue(stepnode, "write_retry_delay");
        if (!Const.isEmpty(writeRetryDelay)) {
            m_writeRetryDelay = writeRetryDelay;
        }

        Node fields = XMLHandler.getSubNode(stepnode, "mongo_fields");
        if (fields != null && XMLHandler.countNodes(fields, "mongo_field") > 0) {
            int nrfields = XMLHandler.countNodes(fields, "mongo_field");
            m_mongoFields = new ArrayList<MongoField>();

            for (int i = 0; i < nrfields; i++) {
                Node fieldNode = XMLHandler.getSubNodeByNr(fields, "mongo_field", i);

                MongoField newField = new MongoField();
                newField.m_mongoDocPath = XMLHandler.getTagValue(fieldNode, "mongo_doc_path");
                newField.m_comparator = XMLHandler.getTagValue(fieldNode, "comparator");
                newField.m_incomingField1 = XMLHandler.getTagValue(fieldNode, "incoming_field_1");
                newField.m_incomingField2 = XMLHandler.getTagValue(fieldNode, "incoming_field_2");

                m_mongoFields.add(newField);
            }
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        setHostnames(rep.getStepAttributeString(id_step, 0, "mongo_host"));
        setPort(rep.getStepAttributeString(id_step, 0, "mongo_port"));
        setUseAllReplicaSetMembers(rep.getStepAttributeBoolean(id_step, 0, "use_all_replica_members"));
        setAuthenticationUser(rep.getStepAttributeString(id_step, 0, "mongo_user"));
        setAuthenticationPassword(rep.getStepAttributeString(id_step, 0, "mongo_password"));
        if (!Const.isEmpty(getAuthenticationPassword())) {
            setAuthenticationPassword(Encr.decryptPasswordOptionallyEncrypted(getAuthenticationPassword()));
        }
        setUseKerberosAuthentication(rep.getStepAttributeBoolean(id_step, "auth_kerberos"));
        setDbName(rep.getStepAttributeString(id_step, 0, "mongo_db"));
        setCollection(rep.getStepAttributeString(id_step, 0, "mongo_collection"));

        setConnectTimeout(rep.getStepAttributeString(id_step, "connect_timeout"));
        setSocketTimeout(rep.getStepAttributeString(id_step, "socket_timeout"));
        setReadPreference(rep.getStepAttributeString(id_step, "read_preference"));
        setWriteConcern(rep.getStepAttributeString(id_step, "write_concern"));
        setWTimeout(rep.getStepAttributeString(id_step, "w_timeout"));
        setJournal(rep.getStepAttributeBoolean(id_step, 0, "journaled_writes"));

        int nrfields = rep.countNrStepAttributes(id_step, "mongo_doc_path");

        String writeRetries = rep.getStepAttributeString(id_step, "write_retries");
        if (!Const.isEmpty(writeRetries)) {
            m_writeRetries = writeRetries;
        }
        String writeRetryDelay = rep.getStepAttributeString(id_step, "write_retry_delay");
        if (!Const.isEmpty(writeRetryDelay)) {
            m_writeRetryDelay = writeRetryDelay;
        }

        if (nrfields > 0) {
            m_mongoFields = new ArrayList<MongoField>();

            for (int i = 0; i < nrfields; i++) {
                MongoField newField = new MongoField();

                newField.m_mongoDocPath = rep.getStepAttributeString(id_step, i, "mongo_doc_path");
                newField.m_comparator = rep.getStepAttributeString(id_step, i, "comparator");
                newField.m_incomingField1 = rep.getStepAttributeString(id_step, i, "incoming_field_1");
                newField.m_incomingField2 = rep.getStepAttributeString(id_step, i, "incoming_field_2");

                m_mongoFields.add(newField);
            }
        }

    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        if (!Const.isEmpty(getHostnames())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_host",
                    getHostnames());
        }
        if (!Const.isEmpty(getPort())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_port", getPort());
        }

        rep.saveStepAttribute(id_transformation, id_step, "use_all_replica_members", getUseAllReplicaSetMembers());

        if (!Const.isEmpty(getAuthenticationUser())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_user",
                    getAuthenticationUser());
        }
        if (!Const.isEmpty(getAuthenticationPassword())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_password",
                    Encr.encryptPasswordIfNotUsingVariables(getAuthenticationPassword()));
        }

        rep.saveStepAttribute(id_transformation, id_step, "auth_kerberos",
                getUseKerberosAuthentication());

        if (!Const.isEmpty(getDbName())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_db", getDbName());
        }
        if (!Const.isEmpty(getCollection())) {
            rep.saveStepAttribute(id_transformation, id_step, 0, "mongo_collection",
                    getCollection());
        }

        rep.saveStepAttribute(id_transformation, id_step, "connect_timeout",
                getConnectTimeout());
        rep.saveStepAttribute(id_transformation, id_step, "socket_timeout",
                getSocketTimeout());
        rep.saveStepAttribute(id_transformation, id_step, "read_preference",
                getReadPreference());
        rep.saveStepAttribute(id_transformation, id_step, "write_concern",
                getWriteConcern());
        rep.saveStepAttribute(id_transformation, id_step, "w_timeout", getWTimeout());
        rep.saveStepAttribute(id_transformation, id_step, "journaled_writes",
                getJournal());

        rep.saveStepAttribute(id_transformation, id_step, 0, "write_retries",
                m_writeRetries);
        rep.saveStepAttribute(id_transformation, id_step, 0, "write_retry_delay",
                m_writeRetryDelay);

        if (m_mongoFields != null && m_mongoFields.size() > 0) {
            for (int i = 0; i < m_mongoFields.size(); i++) {
                MongoField field = m_mongoFields.get(i);

                rep.saveStepAttribute(id_transformation, id_step, i, "mongo_doc_path", field.m_mongoDocPath);
                rep.saveStepAttribute(id_transformation, id_step, i, "comparator", field.m_comparator);
                rep.saveStepAttribute(id_transformation, id_step, i, "incoming_field_1", field.m_incomingField1);
                rep.saveStepAttribute(id_transformation, id_step, i, "incoming_field_2", field.m_incomingField2);
            }
        }
    }
}
