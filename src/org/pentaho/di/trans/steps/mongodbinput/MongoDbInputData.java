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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.mongo.MongoUtils;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

/**
 * @author Matt
 * @author Mark Hall
 * @since 24-jan-2005
 */
public class MongoDbInputData extends BaseStepData implements StepDataInterface {

  public static final int MONGO_DEFAULT_PORT = 27017;

  public RowMetaInterface outputRowMeta;

  public MongoClient mongo;
  public DB db;
  public DBCollection collection;

  public MongoClient m_mongoClient;

  /** cursor for a standard query */
  public DBCursor cursor;

  /** results of an aggregation pipeline */
  Iterator<DBObject> m_pipelineResult;

  private List<MongoField> m_userFields;
  private MongoArrayExpansion m_expansionHandler;

  public static class MongoField implements Comparable<MongoField> {

    /** The name the the field will take in the outputted kettle stream */
    public String m_fieldName = ""; //$NON-NLS-1$

    /** The path to the field in the Mongo object */
    public String m_fieldPath = ""; //$NON-NLS-1$

    /** The kettle type for this field */
    public String m_kettleType = ""; //$NON-NLS-1$

    /** User-defined indexed values for String types */
    public List<String> m_indexedVals;

    /**
     * Temporary variable to hold the min:max array index info for fields
     * determined when sampling documents for paths/types
     */
    public transient String m_arrayIndexInfo;

    /**
     * Temporary variable to hold the number of times this path was seen when
     * sampling documents to determine paths/types.
     */
    private transient int m_percentageOfSample = -1;

    /**
     * Temporary variable to hold the num times this path was seen/num sampled
     * documents. Note that numerator might be larger than denominator if this
     * path is encountered multiple times in an array within one document.
     */
    public transient String m_occurenceFraction = ""; //$NON-NLS-1$

    private transient Object m_mongoType;

    /**
     * Temporary variable used to indicate that this path occurs multiple times
     * over the sampled documents and that the types differ. In this case we
     * should default to Kettle type String as a catch-all
     */
    public transient boolean m_disparateTypes;

    /** The index that this field is in the output row structure */
    protected int m_outputIndex;

    private ValueMeta m_tempValueMeta;

    private List<String> m_pathParts;
    private List<String> m_tempParts;

    public MongoField copy() {
      MongoField newF = new MongoField();
      newF.m_fieldName = m_fieldName;
      newF.m_fieldPath = m_fieldPath;
      newF.m_kettleType = m_kettleType;

      // reference doesn't matter here as this list is read only at runtime
      newF.m_indexedVals = m_indexedVals;

      return newF;
    }

    /**
     * Initialize this mongo field
     * 
     * @param outputIndex the index for this field in the outgoing row
     *          structure.
     * @throws KettleException if a problem occurs
     */
    public void init(int outputIndex) throws KettleException {
      if (Const.isEmpty(m_fieldPath)) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbOutput.Messages.MongoField.Error.NoPathSet")); //$NON-NLS-1$
      }

      if (m_pathParts != null) {
        return;
      }

      String fieldPath = cleansePath(m_fieldPath);

      String[] temp = fieldPath.split("\\."); //$NON-NLS-1$
      m_pathParts = new ArrayList<String>();
      for (String part : temp) {
        m_pathParts.add(part);
      }

      if (m_pathParts.get(0).equals("$")) { //$NON-NLS-1$
        m_pathParts.remove(0); // root record indicator
      } else if (m_pathParts.get(0).startsWith("$[")) { //$NON-NLS-1$

        // strip leading $ off of array
        String r = m_pathParts.get(0).substring(1, m_pathParts.get(0).length());
        m_pathParts.set(0, r);
      }

      m_tempParts = new ArrayList<String>();
      m_tempValueMeta = new ValueMeta();
      m_tempValueMeta.setType(ValueMeta.getType(m_kettleType));
      m_outputIndex = outputIndex;
    }

    /**
     * Reset this field, ready for processing a new document
     * 
     * @param space variables to use
     */
    public void reset(VariableSpace space) {
      // first clear because there may be stuff left over from processing
      // the previous mongo document object (especially if a path exited early
      // due to non-existent field or array index out of bounds)
      m_tempParts.clear();

      for (String part : m_pathParts) {
        m_tempParts.add(space.environmentSubstitute(part));
      }
    }

    /**
     * Perform Kettle type conversions for the Mongo leaf field value.
     * 
     * @param fieldValue the leaf value from the Mongo structure
     * @return an Object of the appropriate Kettle type
     * @throws KettleException if a problem occurs
     */
    protected Object getKettleValue(Object fieldValue) throws KettleException {

      switch (m_tempValueMeta.getType()) {
      case ValueMetaInterface.TYPE_BIGNUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = BigDecimal.valueOf(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Date) {
          fieldValue = new BigDecimal(((Date) fieldValue).getTime());
        } else {
          fieldValue = new BigDecimal(fieldValue.toString());
        }
        return m_tempValueMeta.getBigNumber(fieldValue);
      case ValueMetaInterface.TYPE_BINARY:
        if (fieldValue instanceof Binary) {
          fieldValue = ((Binary) fieldValue).getData();
        } else {
          fieldValue = fieldValue.toString().getBytes();
        }
        return m_tempValueMeta.getBinary(fieldValue);
      case ValueMetaInterface.TYPE_BOOLEAN:
        if (fieldValue instanceof Number) {
          fieldValue = new Boolean(((Number) fieldValue).intValue() != 0);
        } else if (fieldValue instanceof Date) {
          fieldValue = new Boolean(((Date) fieldValue).getTime() != 0);
        } else {
          fieldValue = new Boolean(fieldValue.toString().equalsIgnoreCase("Y") //$NON-NLS-1$
              || fieldValue.toString().equalsIgnoreCase("T") //$NON-NLS-1$
              || fieldValue.toString().equalsIgnoreCase("1")); //$NON-NLS-1$
        }
        return m_tempValueMeta.getBoolean(fieldValue);
      case ValueMetaInterface.TYPE_DATE:
        if (fieldValue instanceof Number) {
          fieldValue = new Date(((Number) fieldValue).longValue());
        } else if (fieldValue instanceof Date) {
          // nothing to do
        } else {
          throw new KettleException(BaseMessages.getString(
              MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.DateConversion", //$NON-NLS-1$
              fieldValue.toString()));
        }
        return m_tempValueMeta.getDate(fieldValue);
      case ValueMetaInterface.TYPE_INTEGER:
        if (fieldValue instanceof Number) {
          fieldValue = new Long(((Number) fieldValue).intValue());
        } else if (fieldValue instanceof Binary) {
          byte[] b = ((Binary) fieldValue).getData();
          String s = new String(b);
          fieldValue = new Integer(s);
        } else {
          fieldValue = new Integer(fieldValue.toString());
        }
        return m_tempValueMeta.getInteger(fieldValue);
      case ValueMetaInterface.TYPE_NUMBER:
        if (fieldValue instanceof Number) {
          fieldValue = new Double(((Number) fieldValue).doubleValue());
        } else if (fieldValue instanceof Binary) {
          byte[] b = ((Binary) fieldValue).getData();
          String s = new String(b);
          fieldValue = new Double(s);
        } else {
          fieldValue = new Double(fieldValue.toString());
        }
        return m_tempValueMeta.getNumber(fieldValue);
      case ValueMetaInterface.TYPE_STRING:
        return m_tempValueMeta.getString(fieldValue);
      default:
        return null;
      }
    }

    /**
     * Convert a mongo record object to a Kettle field value (for the field
     * defined by this path)
     * 
     * @param mongoObject the record to convert
     * @return the kettle field value
     * @throws KettleException if a problem occurs
     */
    public Object convertToKettleValue(BasicDBObject mongoObject)
        throws KettleException {

      if (mongoObject == null) {
        return null;
      }

      if (m_tempParts.size() == 0) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.MalformedPathRecord")); //$NON-NLS-1$
      }

      String part = m_tempParts.remove(0);

      if (part.charAt(0) == '[') {
        // we're not expecting an array at this point - this document does not
        // contain our field
        return null;
      }

      if (part.indexOf('[') > 0) {
        String arrayPart = part.substring(part.indexOf('['));
        part = part.substring(0, part.indexOf('['));

        // put the array section back into location zero
        m_tempParts.add(0, arrayPart);
      }

      // part is a named field of this record
      Object fieldValue = mongoObject.get(part);
      if (fieldValue == null) {
        return null;
      }

      // what have we got
      if (m_tempParts.size() == 0) {
        // we're expecting a leaf primitive - lets see if that's what we have
        // here...
        return getKettleValue(fieldValue);
      }

      if (fieldValue instanceof BasicDBObject) {
        return convertToKettleValue(((BasicDBObject) fieldValue));
      }

      if (fieldValue instanceof BasicDBList) {
        return convertToKettleValue(((BasicDBList) fieldValue));
      }

      // must mean we have a primitive here, but we're expecting to process more
      // path so this doesn't match us - return null
      return null;
    }

    /**
     * Convert a mongo array object to a Kettle field value (for the field
     * defined in this path)
     * 
     * @param mongoObject the array to convert
     * @return the kettle field value
     * @throws KettleException if a problem occurs
     */
    public Object convertToKettleValue(BasicDBList mongoList)
        throws KettleException {

      if (mongoList == null) {
        return null;
      }

      if (m_tempParts.size() == 0) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.MalformedPathArray")); //$NON-NLS-1$
      }

      String part = m_tempParts.remove(0);
      if (!(part.charAt(0) == '[')) {
        // we're expecting an array at this point - this document does not
        // contain our field
        return null;
      }

      String index = part.substring(1, part.indexOf(']'));
      int arrayI = 0;
      try {
        arrayI = Integer.parseInt(index.trim());
      } catch (NumberFormatException e) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index)); //$NON-NLS-1$
      }

      if (part.indexOf(']') < part.length() - 1) {
        // more dimensions to the array
        part = part.substring(part.indexOf(']') + 1, part.length());
        m_tempParts.add(0, part);
      }

      if (arrayI >= mongoList.size() || arrayI < 0) {
        return null;
      }

      Object element = mongoList.get(arrayI);

      if (element == null) {
        return null;
      }

      if (m_tempParts.size() == 0) {
        // we're expecting a leaf primitive - let's see if that's what we have
        // here...
        return getKettleValue(element);
      }

      if (element instanceof BasicDBObject) {
        return convertToKettleValue(((BasicDBObject) element));
      }

      if (element instanceof BasicDBList) {
        return convertToKettleValue(((BasicDBList) element));
      }

      // must mean we have a primitive here, but we're expecting to process more
      // path so this doesn't match us - return null
      return null;
    }

    @Override
    public int compareTo(MongoField comp) {
      return m_fieldName.compareTo(comp.m_fieldName);
    }
  }

  protected static class MongoArrayExpansion {

    /** The prefix of the full path that defines the expansion */
    public String m_expansionPath;

    /**
     * Subfield objects that handle the processing of the path after the
     * expansion prefix
     */
    protected List<MongoField> m_subFields;

    private List<String> m_pathParts;
    private List<String> m_tempParts;

    protected RowMetaInterface m_outputRowMeta;

    public MongoArrayExpansion(List<MongoField> subFields) {
      m_subFields = subFields;
    }

    /**
     * Initialize this field by parsing the path etc.
     * 
     * @throws KettleException if a problem occurs
     */
    public void init() throws KettleException {
      if (Const.isEmpty(m_expansionPath)) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.NoPathSet")); //$NON-NLS-1$
      }
      if (m_pathParts != null) {
        return;
      }

      String expansionPath = cleansePath(m_expansionPath);

      String[] temp = expansionPath.split("\\."); //$NON-NLS-1$
      m_pathParts = new ArrayList<String>();
      for (String part : temp) {
        m_pathParts.add(part);
      }

      if (m_pathParts.get(0).equals("$")) { //$NON-NLS-1$
        m_pathParts.remove(0); // root record indicator
      } else if (m_pathParts.get(0).startsWith("$[")) { //$NON-NLS-1$

        // strip leading $ off of array
        String r = m_pathParts.get(0).substring(1, m_pathParts.get(0).length());
        m_pathParts.set(0, r);
      }
      m_tempParts = new ArrayList<String>();

      // initialize the sub fields
      if (m_subFields != null) {
        for (MongoField f : m_subFields) {
          int outputIndex = m_outputRowMeta.indexOfValue(f.m_fieldName);
          f.init(outputIndex);
        }
      }
    }

    /**
     * Reset this field. Should be called prior to processing a new field value
     * from the avro file
     * 
     * @param space environment variables (values that environment variables
     *          resolve to cannot contain "."s)
     */
    public void reset(VariableSpace space) {
      m_tempParts.clear();

      for (String part : m_pathParts) {
        m_tempParts.add(space.environmentSubstitute(part));
      }

      // reset sub fields
      for (MongoField f : m_subFields) {
        f.reset(space);
      }
    }

    protected Object[][] nullResult() {
      Object[][] result = new Object[1][m_outputRowMeta.size()
          + RowDataUtil.OVER_ALLOCATE_SIZE];

      return result;
    }

    public Object[][] convertToKettleValue(BasicDBObject mongoObject,
        VariableSpace space) throws KettleException {

      if (mongoObject == null) {
        return nullResult();
      }

      if (m_tempParts.size() == 0) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.MalformedPathRecord")); //$NON-NLS-1$
      }

      String part = m_tempParts.remove(0);

      if (part.charAt(0) == '[') {
        // we're not expecting an array at this point - this document does not
        // contain our field(s)
        return nullResult();
      }

      if (part.indexOf('[') > 0) {
        String arrayPart = part.substring(part.indexOf('['));
        part = part.substring(0, part.indexOf('['));

        // put the array section back into location zero
        m_tempParts.add(0, arrayPart);
      }

      // part is a named field of this record
      Object fieldValue = mongoObject.get(part);
      if (fieldValue == null) {
        return nullResult();
      }

      if (fieldValue instanceof BasicDBObject) {
        return convertToKettleValue(((BasicDBObject) fieldValue), space);
      }

      if (fieldValue instanceof BasicDBList) {
        return convertToKettleValue(((BasicDBList) fieldValue), space);
      }

      // must mean we have a primitive here, but we're expecting to process more
      // path so this doesn't match us - return null
      return nullResult();
    }

    public Object[][] convertToKettleValue(BasicDBList mongoList,
        VariableSpace space) throws KettleException {

      if (mongoList == null) {
        return nullResult();
      }

      if (m_tempParts.size() == 0) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoDbInput.ErrorMessage.MalformedPathArray")); //$NON-NLS-1$
      }

      String part = m_tempParts.remove(0);
      if (!(part.charAt(0) == '[')) {
        // we're expecting an array at this point - this document does not
        // contain our field
        return nullResult();
      }

      String index = part.substring(1, part.indexOf(']'));

      if (part.indexOf(']') < part.length() - 1) {
        // more dimensions to the array
        part = part.substring(part.indexOf(']') + 1, part.length());
        m_tempParts.add(0, part);
      }

      if (index.equals("*")) { //$NON-NLS-1$
        // start the expansion - we delegate conversion to our subfields
        Object[][] result = new Object[mongoList.size()][m_outputRowMeta.size()
            + RowDataUtil.OVER_ALLOCATE_SIZE];

        for (int i = 0; i < mongoList.size(); i++) {
          Object element = mongoList.get(i);

          for (int j = 0; j < m_subFields.size(); j++) {
            MongoField sf = m_subFields.get(j);
            sf.reset(space);

            // what have we got?
            if (element instanceof BasicDBObject) {
              result[i][sf.m_outputIndex] = sf
                  .convertToKettleValue((BasicDBObject) element);
            } else if (element instanceof BasicDBList) {
              result[i][sf.m_outputIndex] = sf
                  .convertToKettleValue((BasicDBList) element);
            } else {
              // assume a primitive
              result[i][sf.m_outputIndex] = sf.getKettleValue(element);
            }
          }
        }

        return result;
      } else {
        int arrayI = 0;
        try {
          arrayI = Integer.parseInt(index.trim());
        } catch (NumberFormatException e) {
          throw new KettleException(BaseMessages.getString(
              MongoDbInputMeta.PKG,
              "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index)); //$NON-NLS-1$
        }

        if (arrayI >= mongoList.size() || arrayI < 0) {
          // index is out of bounds
          return nullResult();
        }

        Object element = mongoList.get(arrayI);

        if (element == null) {
          return nullResult();
        }

        if (element instanceof BasicDBObject) {
          return convertToKettleValue(((BasicDBObject) element), space);
        }

        if (element instanceof BasicDBList) {
          return convertToKettleValue(((BasicDBList) element), space);
        }

        // must mean we have a primitive here, but we're expecting to process
        // more
        // path so this doesn't match us - return null
        return nullResult();
      }
    }
  }

  protected static MongoArrayExpansion checkFieldPaths(
      List<MongoField> normalFields, RowMetaInterface outputRowMeta)
      throws KettleException {

    // here we check whether there are any full array expansions
    // specified in the paths (via [*]). If so, we want to make sure
    // that only one is present across all paths. E.g. we can handle
    // multiple fields like $.person[*].first, $.person[*].last etc.
    // but not $.person[*].first, $.person[*].address[*].street.

    String expansion = null;
    List<MongoField> normalList = new ArrayList<MongoField>();
    List<MongoField> expansionList = new ArrayList<MongoField>();

    for (MongoField f : normalFields) {
      String path = f.m_fieldPath;

      if (path != null && path.lastIndexOf("[*]") >= 0) { //$NON-NLS-1$

        if (path.indexOf("[*]") != path.lastIndexOf("[*]")) { //$NON-NLS-1$ //$NON-NLS-2$
          throw new KettleException(BaseMessages.getString(
              MongoDbInputMeta.PKG,
              "MongoInput.ErrorMessage.PathContainsMultipleExpansions", path)); //$NON-NLS-1$
        }

        String pathPart = path.substring(0, path.lastIndexOf("[*]") + 3); //$NON-NLS-1$

        if (expansion == null) {
          expansion = pathPart;
        } else {
          if (!expansion.equals(pathPart)) {
            throw new KettleException(BaseMessages.getString(
                MongoDbInputMeta.PKG,
                "MongoDbInput.ErrorMessage.MutipleDifferentExpansions")); //$NON-NLS-1$
          }
        }

        expansionList.add(f);
      } else {
        normalList.add(f);
      }
    }

    normalFields.clear();
    for (MongoField f : normalList) {
      normalFields.add(f);
    }

    if (expansionList.size() > 0) {

      List<MongoField> subFields = new ArrayList<MongoField>();

      for (MongoField ef : expansionList) {
        MongoField subField = new MongoField();
        subField.m_fieldName = ef.m_fieldName;
        String path = ef.m_fieldPath;
        if (path.charAt(path.length() - 2) == '*') {
          path = "dummy"; // pulling a primitive out of the array (path //$NON-NLS-1$
                          // doesn't matter)
        } else {
          path = path.substring(path.lastIndexOf("[*]") + 3, path.length()); //$NON-NLS-1$
          path = "$" + path; //$NON-NLS-1$
        }

        subField.m_fieldPath = path;
        subField.m_indexedVals = ef.m_indexedVals;
        subField.m_kettleType = ef.m_kettleType;

        subFields.add(subField);
      }

      MongoArrayExpansion exp = new MongoArrayExpansion(subFields);
      exp.m_expansionPath = expansion;
      exp.m_outputRowMeta = outputRowMeta;

      return exp;
    }

    return null;
  }

  public MongoDbInputData() {
    super();
  }

  /**
   * Initialize all the paths by locating the index for their field name in the
   * outgoing row structure.
   * 
   * @throws KettleException
   */
  public void init() throws KettleException {
    if (m_userFields != null) {

      // set up array expansion/unwinding (if necessary)
      m_expansionHandler = checkFieldPaths(m_userFields, outputRowMeta);

      for (MongoField f : m_userFields) {
        int outputIndex = outputRowMeta.indexOfValue(f.m_fieldName);
        f.init(outputIndex);
      }

      if (m_expansionHandler != null) {
        m_expansionHandler.init();
      }
    }
  }

  /**
   * Convert a mongo document to outgoing row field values with respect to the
   * user-specified paths. May return more than one Kettle row if an array is
   * being expanded/unwound
   * 
   * @param mongoObj the mongo document
   * @param space variables to use
   * @return populated Kettle row(s)
   * @throws KettleException if a problem occurs
   */
  public Object[][] mongoDocumentToKettle(DBObject mongoObj, VariableSpace space)
      throws KettleException {

    Object[][] result = null;

    if (m_expansionHandler != null) {
      m_expansionHandler.reset(space);

      if (mongoObj instanceof BasicDBObject) {
        result = m_expansionHandler.convertToKettleValue(
            (BasicDBObject) mongoObj, space);
      } else {
        result = m_expansionHandler.convertToKettleValue(
            (BasicDBList) mongoObj, space);
      }
    } else {
      result = new Object[1][];
    }

    // get the normal (non expansion-related fields)
    Object[] normalData = RowDataUtil.allocateRowData(outputRowMeta.size());
    Object value;
    for (MongoField f : m_userFields) {
      value = null;
      f.reset(space);

      if (mongoObj instanceof BasicDBObject) {
        value = f.convertToKettleValue((BasicDBObject) mongoObj);
      } else if (mongoObj instanceof BasicDBList) {
        value = f.convertToKettleValue((BasicDBList) mongoObj);
      }

      normalData[f.m_outputIndex] = value;
    }

    // copy normal fields over to each expansion row (if necessary)
    if (m_expansionHandler == null) {
      result[0] = normalData;
    } else {
      for (int i = 0; i < result.length; i++) {
        Object[] row = result[i];
        for (MongoField f : m_userFields) {
          row[f.m_outputIndex] = normalData[f.m_outputIndex];
        }
      }
    }

    return result;
  }

  /**
   * Utility method to return a connection to a Mongo database based on
   * parameters provided by the user in the step meta data
   * 
   * @param meta MongoDbInputMeta
   * @param vars variables to use
   * @param log for logging
   * @return a configured MongoClient object
   * @throws KettleException if a problem occurs
   */
  public static MongoClient initConnection(MongoDbInputMeta meta,
      VariableSpace vars, LogChannelInterface log) throws KettleException {

    return MongoUtils.initConnection(meta, vars, log);
  }

  /**
   * Cleanses a string path by ensuring that any variables names present in the
   * path do not contain "."s (replaces any dots with underscores).
   * 
   * @param path the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath(String path) {
    // look for variables and convert any "." to "_"

    int index = path.indexOf("${"); //$NON-NLS-1$

    int endIndex = 0;
    String tempStr = path;
    while (index >= 0) {
      index += 2;
      endIndex += tempStr.indexOf("}"); //$NON-NLS-1$
      if (endIndex > 0 && endIndex > index + 1) {
        String key = path.substring(index, endIndex);

        String cleanKey = key.replace('.', '_');
        path = path.replace(key, cleanKey);
      } else {
        break;
      }

      if (endIndex + 1 < path.length()) {
        tempStr = path.substring(endIndex + 1, path.length());
      } else {
        break;
      }

      index = tempStr.indexOf("${"); //$NON-NLS-1$

      if (index > 0) {
        index += endIndex;
      }
    }

    return path;
  }

  /**
   * Set user-defined paths for extracting field values from Mongo documents
   * 
   * @param fields the field path specifications
   */
  public void setMongoFields(List<MongoField> fields) {
    // copy this list
    m_userFields = new ArrayList<MongoField>();

    for (MongoField f : fields) {
      m_userFields.add(f.copy());
    }
  }

  protected static int mongoToKettleType(Object fieldValue) {
    if (fieldValue == null) {
      return ValueMetaInterface.TYPE_STRING;
    }

    if (fieldValue instanceof Symbol || fieldValue instanceof String
        || fieldValue instanceof Code || fieldValue instanceof ObjectId
        || fieldValue instanceof MinKey || fieldValue instanceof MaxKey) {
      return ValueMetaInterface.TYPE_STRING;
    } else if (fieldValue instanceof Date) {
      return ValueMetaInterface.TYPE_DATE;
    } else if (fieldValue instanceof Number) {
      // try to parse as an Integer
      try {
        Integer.parseInt(fieldValue.toString());
        return ValueMetaInterface.TYPE_INTEGER;
      } catch (NumberFormatException e) {
        return ValueMetaInterface.TYPE_NUMBER;
      }
    } else if (fieldValue instanceof Binary) {
      return ValueMetaInterface.TYPE_BINARY;
    } else if (fieldValue instanceof BSONTimestamp) {
      return ValueMetaInterface.TYPE_INTEGER;
    }

    return ValueMetaInterface.TYPE_STRING;
  }

  protected static void setMinArrayIndexes(MongoField m) {
    // set the actual index for each array in the path to the
    // corresponding minimum index
    // recorded in the name

    if (m.m_fieldName.indexOf('[') < 0) {
      return;
    }

    String temp = m.m_fieldPath;
    String tempComp = m.m_fieldName;
    StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      String firstPart = temp.substring(0, temp.indexOf('['));
      String innerPart = temp.substring(temp.indexOf('[') + 1,
          temp.indexOf(']'));

      if (!innerPart.equals("-")) { //$NON-NLS-1$
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append(firstPart);

        String innerComp = tempComp.substring(tempComp.indexOf('[') + 1,
            tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1,
              tempComp.length());
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] compParts = innerComp.split(":"); //$NON-NLS-1$
        String replace = "[" + compParts[0] + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        updated.append(replace);

      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.m_fieldPath = updated.toString();
  }

  protected static void updateMaxArrayIndexes(MongoField m, String update) {
    // just look at the second (i.e. max index value) in the array parts
    // of update
    if (m.m_fieldName.indexOf('[') < 0) {
      return;
    }

    if (m.m_fieldName.split("\\[").length != update.split("\\[").length) { //$NON-NLS-1$ //$NON-NLS-2$
      throw new IllegalArgumentException(
          "Field path and update path do not seem to contain " //$NON-NLS-1$
              + "the same number of array parts!"); //$NON-NLS-1$
    }

    String temp = m.m_fieldName;
    String tempComp = update;
    StringBuffer updated = new StringBuffer();

    while (temp.indexOf('[') >= 0) {
      String firstPart = temp.substring(0, temp.indexOf('['));
      String innerPart = temp.substring(temp.indexOf('[') + 1,
          temp.indexOf(']'));

      if (innerPart.indexOf(':') < 0) {
        // terminal primitive specific index
        updated.append(temp); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append(firstPart);

        String innerComp = tempComp.substring(tempComp.indexOf('[') + 1,
            tempComp.indexOf(']'));

        if (temp.indexOf(']') < temp.length() - 1) {
          temp = temp.substring(temp.indexOf(']') + 1, temp.length());
          tempComp = tempComp.substring(tempComp.indexOf(']') + 1,
              tempComp.length());
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] origParts = innerPart.split(":"); //$NON-NLS-1$
        String[] compParts = innerComp.split(":"); //$NON-NLS-1$
        int origMax = Integer.parseInt(origParts[1]);
        int compMax = Integer.parseInt(compParts[1]);

        if (compMax > origMax) {
          // updated the max index seen for this path
          String newRange = "[" + origParts[0] + ":" + compMax + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          updated.append(newRange);
        } else {
          String oldRange = "[" + innerPart + "]"; //$NON-NLS-1$ //$NON-NLS-2$
          updated.append(oldRange);
        }
      }
    }

    if (temp.length() > 0) {
      // append remaining part
      updated.append(temp);
    }

    m.m_fieldName = updated.toString();
  }

  protected static void docToFields(DBObject doc, Map<String, MongoField> lookup) {
    String root = "$"; //$NON-NLS-1$
    String name = "$"; //$NON-NLS-1$

    if (doc instanceof BasicDBObject) {
      processRecord((BasicDBObject) doc, root, name, lookup);
    } else if (doc instanceof BasicDBList) {
      processList((BasicDBList) doc, root, name, lookup);
    }
  }

  protected static void processRecord(BasicDBObject rec, String path,
      String name, Map<String, MongoField> lookup) {
    for (String key : rec.keySet()) {
      Object fieldValue = rec.get(key);

      if (fieldValue instanceof BasicDBObject) {
        processRecord((BasicDBObject) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup);
      } else if (fieldValue instanceof BasicDBList) {
        processList((BasicDBList) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup);
      } else {
        // some sort of primitive
        String finalPath = path + "." + key; //$NON-NLS-1$
        String finalName = name + "." + key; //$NON-NLS-1$
        if (!lookup.containsKey(finalPath)) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType(fieldValue);
          newField.m_mongoType = fieldValue;
          newField.m_fieldName = finalName;
          newField.m_fieldPath = finalPath;
          newField.m_kettleType = ValueMeta.getTypeDesc(kettleType);
          newField.m_percentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get(finalPath);
          if (!m.m_mongoType.getClass().isAssignableFrom(fieldValue.getClass())) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  protected static void processList(BasicDBList list, String path, String name,
      Map<String, MongoField> lookup) {

    if (list.size() == 0) {
      return; // can't infer anything about an empty list
    }

    String nonPrimitivePath = path + "[-]"; //$NON-NLS-1$
    String primitivePath = path;

    for (int i = 0; i < list.size(); i++) {
      Object element = list.get(i);

      if (element instanceof BasicDBObject) {
        processRecord((BasicDBObject) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup); //$NON-NLS-1$ //$NON-NLS-2$
      } else if (element instanceof BasicDBList) {
        processList((BasicDBList) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup); //$NON-NLS-1$ //$NON-NLS-2$
      } else {
        // some sort of primitive
        String finalPath = primitivePath + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        String finalName = name + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        if (!lookup.containsKey(finalPath)) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType(element);
          newField.m_mongoType = element;
          newField.m_fieldName = finalPath;
          newField.m_fieldPath = finalName;
          newField.m_kettleType = ValueMeta.getTypeDesc(kettleType);
          newField.m_percentageOfSample = 1;

          lookup.put(finalPath, newField);
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get(finalPath);
          if (!m.m_mongoType.getClass().isAssignableFrom(element.getClass())) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes(m, finalName);
        }
      }
    }
  }

  protected static void postProcessPaths(Map<String, MongoField> fieldLookup,
      List<MongoField> discoveredFields, int numDocsProcessed) {
    for (String key : fieldLookup.keySet()) {
      MongoField m = fieldLookup.get(key);
      m.m_occurenceFraction = "" + m.m_percentageOfSample + "/" //$NON-NLS-1$ //$NON-NLS-2$
          + numDocsProcessed;
      setMinArrayIndexes(m);

      // set field names to terminal part and copy any min:max array index
      // info
      if (m.m_fieldName.contains("[") && m.m_fieldName.contains(":")) { //$NON-NLS-1$ //$NON-NLS-2$
        m.m_arrayIndexInfo = m.m_fieldName;
      }
      if (m.m_fieldName.indexOf('.') >= 0) {
        m.m_fieldName = m.m_fieldName.substring(
            m.m_fieldName.lastIndexOf('.') + 1, m.m_fieldName.length());
      }

      if (m.m_disparateTypes) {
        // force type to string if we've seen this path more than once
        // with incompatible types
        m.m_kettleType = ValueMeta.getTypeDesc(ValueMeta.TYPE_STRING);
      }
      discoveredFields.add(m);
    }

    // check for name clashes
    Map<String, Integer> tempM = new HashMap<String, Integer>();
    for (MongoField m : discoveredFields) {
      if (tempM.get(m.m_fieldName) != null) {
        Integer toUse = tempM.get(m.m_fieldName);
        String key = m.m_fieldName;
        m.m_fieldName = key + "_" + toUse; //$NON-NLS-1$
        toUse = new Integer(toUse.intValue() + 1);
        tempM.put(key, toUse);
      } else {
        tempM.put(m.m_fieldName, 1);
      }
    }
  }

  private static Iterator<DBObject> setUpPipelineSample(String query,
      int numDocsToSample, DBCollection collection) throws KettleException {

    query = query + ", {$limit : " + numDocsToSample + "}"; //$NON-NLS-1$ //$NON-NLS-2$
    List<DBObject> samplePipe = jsonPipelineToDBObjectList(query);

    DBObject first = samplePipe.get(0);
    DBObject[] remainder = new DBObject[samplePipe.size() - 1];
    for (int i = 1; i < samplePipe.size(); i++) {
      remainder[i - 1] = samplePipe.get(i);
    }

    AggregationOutput result = collection.aggregate(first, remainder);

    return result.results().iterator();
  }

  public static boolean discoverFields(MongoDbInputMeta meta,
      VariableSpace vars, int numDocsToSample) throws KettleException {

    if (numDocsToSample < 1) {
      numDocsToSample = 100; // default
    }

    DBCursor cursor = null;
    MongoClient mongo = null;
    String db = vars.environmentSubstitute(meta.getDbName());
    String collection = vars.environmentSubstitute(meta.getCollection());

    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
    try {
      mongo = initConnection(meta, vars, null);
      if (Const.isEmpty(db)) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoInput.ErrorMessage.NoDBSpecified")); //$NON-NLS-1$
      }
      DB database = mongo.getDB(db);

      String realUser = vars
          .environmentSubstitute(meta.getAuthenticationUser());
      String realPass = Encr.decryptPasswordOptionallyEncrypted(vars
          .environmentSubstitute(meta.getAuthenticationPassword()));

      if (!Const.isEmpty(realUser) || !Const.isEmpty(realPass)) {
        if (!database.authenticate(realUser, realPass.toCharArray())) {
          throw new KettleException(BaseMessages.getString(
              MongoDbInputMeta.PKG,
              "MongoDbInput.ErrorAuthenticating.Exception")); //$NON-NLS-1$
        }
      }
      if (Const.isEmpty(collection)) {
        throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
            "MongoInput.ErrorMessage.NoCollectionSpecified")); //$NON-NLS-1$
      }
      DBCollection dbcollection = database.getCollection(collection);

      String query = vars.environmentSubstitute(meta.getJsonQuery());
      String fields = vars.environmentSubstitute(meta.getFieldsName());

      cursor = null;
      Iterator<DBObject> pipeSample = null;

      if (meta.getQueryIsPipeline()) {
        pipeSample = setUpPipelineSample(query, numDocsToSample, dbcollection);
      } else {
        if (Const.isEmpty(query) && Const.isEmpty(fields)) {
          cursor = dbcollection.find().limit(numDocsToSample);
        } else {
          DBObject dbObject = (DBObject) JSON.parse(Const.isEmpty(query) ? "{}" //$NON-NLS-1$
              : query);
          DBObject dbObject2 = (DBObject) JSON.parse(fields);
          cursor = dbcollection.find(dbObject, dbObject2)
              .limit(numDocsToSample);
        }
      }

      int actualCount = 0;
      while (cursor != null ? cursor.hasNext() : pipeSample.hasNext()) {
        actualCount++;
        DBObject nextDoc = (cursor != null ? cursor.next() : pipeSample.next());
        docToFields(nextDoc, fieldLookup);
      }

      postProcessPaths(fieldLookup, discoveredFields, actualCount);

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if (discoveredFields.size() > 0) {
        meta.setMongoFields(discoveredFields);

        return true;
      }
    } catch (Exception e) {
      throw new KettleException(e);
    } finally {
      if (cursor != null) {
        cursor.close();
      }

      if (mongo != null) {
        mongo.close();
      }
    }

    return false;
  }

  protected static List<DBObject> jsonPipelineToDBObjectList(String jsonPipeline)
      throws KettleException {
    List<DBObject> pipeline = new ArrayList<DBObject>();
    StringBuilder b = new StringBuilder(jsonPipeline.trim());

    // extract the parts of the pipeline
    int bracketCount = -1;
    List<String> parts = new ArrayList<String>();
    int i = 0;
    while (i < b.length()) {
      if (b.charAt(i) == '{') {
        if (bracketCount == -1) {
          // trim anything off before this point
          b.delete(0, i);
          bracketCount = 0;
          i = 0;
        }
        bracketCount++;
      }
      if (b.charAt(i) == '}') {
        bracketCount--;
      }
      if (bracketCount == 0) {
        String part = b.substring(0, i + 1);
        parts.add(part);
        bracketCount = -1;

        if (i == b.length() - 1) {
          break;
        }
        b.delete(0, i + 1);
        i = 0;
      }

      i++;
    }

    for (String p : parts) {
      if (!Const.isEmpty(p)) {
        DBObject o = (DBObject) JSON.parse(p);
        pipeline.add(o);
      }
    }

    if (pipeline.size() == 0) {
      throw new KettleException(BaseMessages.getString(MongoDbInputMeta.PKG,
          "MongoDbInput.ErrorMessage.UnableToParsePipelineOperators")); //$NON-NLS-1$
    }

    return pipeline;
  }

  /**
   * Helper function that takes a list of indexed values and returns them as a
   * String in comma-separated form.
   * 
   * @param indexedVals a list of indexed values
   * @return the list a String in comma-separated form
   */
  public static String indexedValsList(List<String> indexedVals) {
    StringBuffer temp = new StringBuffer();

    for (int i = 0; i < indexedVals.size(); i++) {
      temp.append(indexedVals.get(i));
      if (i < indexedVals.size() - 1) {
        temp.append(","); //$NON-NLS-1$
      }
    }

    return temp.toString();
  }

  /**
   * Helper function that takes a comma-separated list in a String and returns a
   * list.
   * 
   * @param indexedVals the String containing the lsit
   * @return a List containing the values
   */
  public static List<String> indexedValsList(String indexedVals) {

    String[] parts = indexedVals.split(","); //$NON-NLS-1$
    List<String> list = new ArrayList<String>();
    for (String s : parts) {
      list.add(s.trim());
    }

    return list;
  }
}
