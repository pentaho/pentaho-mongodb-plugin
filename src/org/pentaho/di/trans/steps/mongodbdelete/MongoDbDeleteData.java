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
import java.util.Date;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Data class for the MongoDbDelete step
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
public class MongoDbDeleteData extends BaseStepData implements StepDataInterface {

    private static Class<?> PKG = MongoDbDeleteMeta.class;
    public static final int MONGO_DEFAULT_PORT = 27017;
    public RowMetaInterface outputRowMeta;
    public MongoClientWrapper clientWrapper;
    // public DB db;
    public MongoCollectionWrapper collection;
    /** cursor for a standard query */
    public MongoCursorWrapper cursor;
    protected List<MongoDbDeleteMeta.MongoField> m_userFields;

    /**
     * Initialize all the paths by locating the index for their field name in the outgoing row structure.
     *
     * @throws KettleException
     */
    public void init(VariableSpace vars) throws KettleException {
        if (m_userFields != null) {
            for (MongoDbDeleteMeta.MongoField f : m_userFields) {
                f.init(vars);
            }
        }
    }

    /**
     * Get the current connection or null if not connected
     *
     * @return the connection or null
     */
    public MongoClientWrapper getConnection() {
        return clientWrapper;
    }

    /**
     * Set the current connection
     *
     * @param clientWrapper
     *                      the connection to use
     */
    public void setConnection(MongoClientWrapper clientWrapper) {
        this.clientWrapper = clientWrapper;
    }

    /**
     * Create a collection in the current database
     *
     * @param collectionName
     *                       the name of the collection to create
     * @throws Exception
     * if a problem occurs
     */
    public void createCollection(String db, String collectionName) throws Exception {
        if (clientWrapper == null) {
            throw new Exception(BaseMessages.getString(PKG, "MongoDelete.ErrorMessage.NoDatabaseSet")); //$NON-NLS-1$
        }

        clientWrapper.createCollection(db, collectionName);
    }

    /**
     * Set the collection to use
     *
     * @param col
     *            the collection to use
     */
    public void setCollection(MongoCollectionWrapper col) {
        collection = col;
    }

    /**
     * Get the collection in use
     *
     * @return the collection in use
     */
    public MongoCollectionWrapper getCollection() {
        return collection;
    }

    /**
     * Set the output row format
     *
     * @param outM
     *             the output row format
     */
    public void setOutputRowMeta(RowMetaInterface outM) {
        outputRowMeta = outM;
    }

    /**
     * Get the output row format
     *
     * @return the output row format
     */
    public RowMetaInterface getOutputRowMeta() {
        return outputRowMeta;
    }

    /**
     * Set the field paths to use for creating the document structure
     *
     * @param fields
     *               the field paths to use
     */
    public void setMongoFields(List<MongoDbDeleteMeta.MongoField> fields) {
        // copy this list
        m_userFields = new ArrayList<MongoDbDeleteMeta.MongoField>();

        for (MongoDbDeleteMeta.MongoField f : fields) {
            m_userFields.add(f.copy());
        }
    }

    public static DBObject getQueryObject(List<MongoDbDeleteMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
            Object[] row, VariableSpace vars) throws KettleException {

        DBObject query = new BasicDBObject();

        boolean haveMatchFields = false;
        boolean hasNonNullMatchValues = false;

        for (MongoDbDeleteMeta.MongoField field : fieldDefs) {
            haveMatchFields = true;

            hasNonNullMatchValues = true;

            String mongoPath = field.m_mongoDocPath;
            String path = vars.environmentSubstitute(mongoPath);
            boolean hasPath = !Const.isEmpty(path);

            if (!hasPath) {
                throw new KettleException(BaseMessages.getString(PKG, "MongoDelete.ErrorMessage.NoMongoPathsDefined"));
            }

            // post process arrays to fit the dot notation (if not already done
            // by the user)
            if (path.indexOf('[') > 0) {
                path = path.replace("[", ".").replace("]", "");
            }

            if (Comparator.EQUAL.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }

                setMongoValueFromKettleValue(query, path, vm, row[index]);
            } else if (Comparator.NOT_EQUAL.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject notEqual = new BasicDBObject();
                setMongoValueFromKettleValue(notEqual, "$ne", vm, row[index]);
                query.put(path.toString(), notEqual);
            } else if (Comparator.GREATER_THAN.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject greaterThan = new BasicDBObject();
                setMongoValueFromKettleValue(greaterThan, "$gt", vm, row[index]);
                query.put(path.toString(), greaterThan);

            } else if(Comparator.GREATER_THAN_EQUAL.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject greaterThanEqual = new BasicDBObject();
                setMongoValueFromKettleValue(greaterThanEqual, "$gte", vm, row[index]);
                query.put(path.toString(), greaterThanEqual);
            } else if (Comparator.LESS_THAN.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject lessThan = new BasicDBObject();
                setMongoValueFromKettleValue(lessThan, "$lt", vm, row[index]);
                query.put(path.toString(), lessThan);
            } else if (Comparator.LESS_THAN_EQUAL.getValue().equals(field.m_comparator)) {
                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm = inputMeta.getValueMeta(index);

                // ignore null fields
                if (vm.isNull(row[index])) {
                    continue;
                }
                DBObject lessThanEqual = new BasicDBObject();
                setMongoValueFromKettleValue(lessThanEqual, "$lte", vm, row[index]);
                query.put(path.toString(), lessThanEqual);
            } else if (Comparator.BETWEEN.getValue().equals(field.m_comparator)) {

                if (Const.isEmpty(field.m_incomingField1) || Const.isEmpty(field.m_incomingField2)) {
                    throw new KettleException(BaseMessages.getString(PKG, "MongoDelete.ErrorMessage.BetweenTwoFieldsRequired"));
                }

                String field1 = vars.environmentSubstitute(field.m_incomingField1);
                int index1 = inputMeta.indexOfValue(field1);
                ValueMetaInterface vm1 = inputMeta.getValueMeta(index1);

                String field2 = vars.environmentSubstitute(field.m_incomingField2);
                int index2 = inputMeta.indexOfValue(field2);
                ValueMetaInterface vm2 = inputMeta.getValueMeta(index2);

                // ignore null fields
                if (vm1.isNull(row[index1]) && vm2.isNull(row[index2])) {
                    continue;
                }

                BasicDBObject between = new BasicDBObject();
                setMongoValueFromKettleValue(between, "$gt", vm1, row[index1]);
                setMongoValueFromKettleValue(between, "$lt", vm2, row[index2]);
                query.put(path.toString(), between);

            } else if (Comparator.IS_NULL.getValue().equals(field.m_comparator)) {
                BasicDBObject exist = new BasicDBObject();
                exist.put("$exists", false);
                query.put(path.toString(), exist);
            } else if (Comparator.IS_NOT_NULL.getValue().equals(field.m_comparator)) {
                BasicDBObject exist = new BasicDBObject();
                exist.put("$exists", true);
                query.put(path.toString(), exist);
            } else {
                throw new KettleException(BaseMessages.getString(PKG, "MongoDelete.ErrorMessage.ComparatorNotSupported", new String[]{field.m_comparator}));
            }

        }

        if (!haveMatchFields) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "MongoDelete.ErrorMessage.NoFieldsToDeleteSpecifiedForMatch"));
        }

        if (!hasNonNullMatchValues) {
            return null;
        }

        return query;
    }

    private static boolean setMongoValueFromKettleValue(DBObject mongoObject, Object lookup,
            ValueMetaInterface kettleType, Object kettleValue) throws KettleValueException {
        if (kettleType.isNull(kettleValue)) {
            return false; // don't insert nulls!
        }

        if (kettleType.isString()) {
            String val = kettleType.getString(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isBoolean()) {
            Boolean val = kettleType.getBoolean(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isInteger()) {
            Long val = kettleType.getInteger(kettleValue);
            mongoObject.put(lookup.toString(), val.longValue());
            return true;
        }
        if (kettleType.isDate()) {
            Date val = kettleType.getDate(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isNumber()) {
            Double val = kettleType.getNumber(kettleValue);
            mongoObject.put(lookup.toString(), val.doubleValue());
            return true;
        }
        if (kettleType.isBigNumber()) {
            // use string value - user can use Kettle to convert back
            String val = kettleType.getString(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isBinary()) {
            byte[] val = kettleType.getBinary(kettleValue);
            mongoObject.put(lookup.toString(), val);
            return true;
        }
        if (kettleType.isSerializableType()) {
            throw new KettleValueException(BaseMessages.getString(PKG,
                    "MongoDelete.ErrorMessage.CantStoreKettleSerializableVals"));
        }

        return false;
    }
}
