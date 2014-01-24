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

package org.pentaho.di.trans.steps.mongodbinput;

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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoClientWrapperFactory;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;
import org.pentaho.mongo.wrapper.field.MongoArrayExpansion;
import org.pentaho.mongo.wrapper.field.MongoField;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * @author Matt
 * @author Mark Hall
 * @since 24-jan-2005
 */
public class MongoDbInputData extends BaseStepData implements StepDataInterface {

  public static final int MONGO_DEFAULT_PORT = 27017;

  public RowMetaInterface outputRowMeta;

  public MongoClientWrapper clientWrapper;
  // public DB db;
  public MongoCollectionWrapper collection;

  /** cursor for a standard query */
  public MongoCursorWrapper cursor;

  /** results of an aggregation pipeline */
  Iterator<DBObject> m_pipelineResult;

  private List<MongoField> m_userFields;
  private MongoArrayExpansion m_expansionHandler;

  protected static MongoArrayExpansion checkFieldPaths( List<MongoField> normalFields, RowMetaInterface outputRowMeta )
    throws KettleException {

    // here we check whether there are any full array expansions
    // specified in the paths (via [*]). If so, we want to make sure
    // that only one is present across all paths. E.g. we can handle
    // multiple fields like $.person[*].first, $.person[*].last etc.
    // but not $.person[*].first, $.person[*].address[*].street.

    String expansion = null;
    List<MongoField> normalList = new ArrayList<MongoField>();
    List<MongoField> expansionList = new ArrayList<MongoField>();

    for ( MongoField f : normalFields ) {
      String path = f.m_fieldPath;

      if ( path != null && path.lastIndexOf( "[*]" ) >= 0 ) { //$NON-NLS-1$

        if ( path.indexOf( "[*]" ) != path.lastIndexOf( "[*]" ) ) { //$NON-NLS-1$ //$NON-NLS-2$
          throw new KettleException( BaseMessages.getString( MongoDbInputMeta.PKG,
              "MongoInput.ErrorMessage.PathContainsMultipleExpansions", path ) ); //$NON-NLS-1$
        }

        String pathPart = path.substring( 0, path.lastIndexOf( "[*]" ) + 3 ); //$NON-NLS-1$

        if ( expansion == null ) {
          expansion = pathPart;
        } else {
          if ( !expansion.equals( pathPart ) ) {
            throw new KettleException( BaseMessages.getString( MongoDbInputMeta.PKG,
                "MongoDbInput.ErrorMessage.MutipleDifferentExpansions" ) ); //$NON-NLS-1$
          }
        }

        expansionList.add( f );
      } else {
        normalList.add( f );
      }
    }

    normalFields.clear();
    for ( MongoField f : normalList ) {
      normalFields.add( f );
    }

    if ( expansionList.size() > 0 ) {

      List<MongoField> subFields = new ArrayList<MongoField>();

      for ( MongoField ef : expansionList ) {
        MongoField subField = new MongoField();
        subField.m_fieldName = ef.m_fieldName;
        String path = ef.m_fieldPath;
        if ( path.charAt( path.length() - 2 ) == '*' ) {
          path = "dummy"; // pulling a primitive out of the array (path //$NON-NLS-1$
                          // doesn't matter)
        } else {
          path = path.substring( path.lastIndexOf( "[*]" ) + 3, path.length() ); //$NON-NLS-1$
          path = "$" + path; //$NON-NLS-1$
        }

        subField.m_fieldPath = path;
        subField.m_indexedVals = ef.m_indexedVals;
        subField.m_kettleType = ef.m_kettleType;

        subFields.add( subField );
      }

      MongoArrayExpansion exp = new MongoArrayExpansion( subFields );
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
   * Initialize all the paths by locating the index for their field name in the outgoing row structure.
   * 
   * @throws KettleException
   */
  public void init() throws KettleException {
    if ( m_userFields != null ) {

      // set up array expansion/unwinding (if necessary)
      m_expansionHandler = checkFieldPaths( m_userFields, outputRowMeta );

      for ( MongoField f : m_userFields ) {
        int outputIndex = outputRowMeta.indexOfValue( f.m_fieldName );
        f.init( outputIndex );
      }

      if ( m_expansionHandler != null ) {
        m_expansionHandler.init();
      }
    }
  }

  /**
   * Convert a mongo document to outgoing row field values with respect to the user-specified paths. May return more
   * than one Kettle row if an array is being expanded/unwound
   * 
   * @param mongoObj
   *          the mongo document
   * @param space
   *          variables to use
   * @return populated Kettle row(s)
   * @throws KettleException
   *           if a problem occurs
   */
  public Object[][] mongoDocumentToKettle( DBObject mongoObj, VariableSpace space ) throws KettleException {

    Object[][] result = null;

    if ( m_expansionHandler != null ) {
      m_expansionHandler.reset( space );

      if ( mongoObj instanceof BasicDBObject ) {
        result = m_expansionHandler.convertToKettleValue( (BasicDBObject) mongoObj, space );
      } else {
        result = m_expansionHandler.convertToKettleValue( (BasicDBList) mongoObj, space );
      }
    } else {
      result = new Object[1][];
    }

    // get the normal (non expansion-related fields)
    Object[] normalData = RowDataUtil.allocateRowData( outputRowMeta.size() );
    Object value;
    for ( MongoField f : m_userFields ) {
      value = null;
      f.reset( space );

      if ( mongoObj instanceof BasicDBObject ) {
        value = f.convertToKettleValue( (BasicDBObject) mongoObj );
      } else if ( mongoObj instanceof BasicDBList ) {
        value = f.convertToKettleValue( (BasicDBList) mongoObj );
      }

      normalData[f.m_outputIndex] = value;
    }

    // copy normal fields over to each expansion row (if necessary)
    if ( m_expansionHandler == null ) {
      result[0] = normalData;
    } else {
      for ( int i = 0; i < result.length; i++ ) {
        Object[] row = result[i];
        for ( MongoField f : m_userFields ) {
          row[f.m_outputIndex] = normalData[f.m_outputIndex];
        }
      }
    }

    return result;
  }

  /**
   * Cleanses a string path by ensuring that any variables names present in the path do not contain "."s (replaces any
   * dots with underscores).
   * 
   * @param path
   *          the path to cleanse
   * @return the cleansed path
   */
  public static String cleansePath( String path ) {
    // look for variables and convert any "." to "_"

    int index = path.indexOf( "${" ); //$NON-NLS-1$

    int endIndex = 0;
    String tempStr = path;
    while ( index >= 0 ) {
      index += 2;
      endIndex += tempStr.indexOf( "}" ); //$NON-NLS-1$
      if ( endIndex > 0 && endIndex > index + 1 ) {
        String key = path.substring( index, endIndex );

        String cleanKey = key.replace( '.', '_' );
        path = path.replace( key, cleanKey );
      } else {
        break;
      }

      if ( endIndex + 1 < path.length() ) {
        tempStr = path.substring( endIndex + 1, path.length() );
      } else {
        break;
      }

      index = tempStr.indexOf( "${" ); //$NON-NLS-1$

      if ( index > 0 ) {
        index += endIndex;
      }
    }

    return path;
  }

  /**
   * Set user-defined paths for extracting field values from Mongo documents
   * 
   * @param fields
   *          the field path specifications
   */
  public void setMongoFields( List<MongoField> fields ) {
    // copy this list
    m_userFields = new ArrayList<MongoField>();

    for ( MongoField f : fields ) {
      m_userFields.add( f.copy() );
    }
  }

  protected static int mongoToKettleType( Object fieldValue ) {
    if ( fieldValue == null ) {
      return ValueMetaInterface.TYPE_STRING;
    }

    if ( fieldValue instanceof Symbol || fieldValue instanceof String || fieldValue instanceof Code
        || fieldValue instanceof ObjectId || fieldValue instanceof MinKey || fieldValue instanceof MaxKey ) {
      return ValueMetaInterface.TYPE_STRING;
    } else if ( fieldValue instanceof Date ) {
      return ValueMetaInterface.TYPE_DATE;
    } else if ( fieldValue instanceof Number ) {
      // try to parse as an Integer
      try {
        Integer.parseInt( fieldValue.toString() );
        return ValueMetaInterface.TYPE_INTEGER;
      } catch ( NumberFormatException e ) {
        return ValueMetaInterface.TYPE_NUMBER;
      }
    } else if ( fieldValue instanceof Binary ) {
      return ValueMetaInterface.TYPE_BINARY;
    } else if ( fieldValue instanceof BSONTimestamp ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }

    return ValueMetaInterface.TYPE_STRING;
  }

  protected static void setMinArrayIndexes( MongoField m ) {
    // set the actual index for each array in the path to the
    // corresponding minimum index
    // recorded in the name

    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    String temp = m.m_fieldPath;
    String tempComp = m.m_fieldName;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( !innerPart.equals( "-" ) ) { //$NON-NLS-1$
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1, temp.length() );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1, tempComp.length() );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        String replace = "[" + compParts[0] + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        updated.append( replace );

      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldPath = updated.toString();
  }

  protected static void updateMaxArrayIndexes( MongoField m, String update ) {
    // just look at the second (i.e. max index value) in the array parts
    // of update
    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    if ( m.m_fieldName.split( "\\[" ).length != update.split( "\\[" ).length ) { //$NON-NLS-1$ //$NON-NLS-2$
      throw new IllegalArgumentException( "Field path and update path do not seem to contain " //$NON-NLS-1$
          + "the same number of array parts!" ); //$NON-NLS-1$
    }

    String temp = m.m_fieldName;
    String tempComp = update;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( innerPart.indexOf( ':' ) < 0 ) {
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1, temp.length() );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1, tempComp.length() );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] origParts = innerPart.split( ":" ); //$NON-NLS-1$
        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        int origMax = Integer.parseInt( origParts[1] );
        int compMax = Integer.parseInt( compParts[1] );

        if ( compMax > origMax ) {
          // updated the max index seen for this path
          String newRange = "[" + origParts[0] + ":" + compMax + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          updated.append( newRange );
        } else {
          String oldRange = "[" + innerPart + "]"; //$NON-NLS-1$ //$NON-NLS-2$
          updated.append( oldRange );
        }
      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldName = updated.toString();
  }

  protected static void docToFields( DBObject doc, Map<String, MongoField> lookup ) {
    String root = "$"; //$NON-NLS-1$
    String name = "$"; //$NON-NLS-1$

    if ( doc instanceof BasicDBObject ) {
      processRecord( (BasicDBObject) doc, root, name, lookup );
    } else if ( doc instanceof BasicDBList ) {
      processList( (BasicDBList) doc, root, name, lookup );
    }
  }

  protected static void processRecord( BasicDBObject rec, String path, String name, Map<String, MongoField> lookup ) {
    for ( String key : rec.keySet() ) {
      Object fieldValue = rec.get( key );

      if ( fieldValue instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup );
      } else if ( fieldValue instanceof BasicDBList ) {
        processList( (BasicDBList) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup );
      } else {
        // some sort of primitive
        String finalPath = path + "." + key; //$NON-NLS-1$
        String finalName = name + "." + key; //$NON-NLS-1$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( fieldValue );
          newField.m_mongoType = fieldValue;
          newField.m_fieldName = finalName;
          newField.m_fieldPath = finalPath;
          newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          if ( !m.m_mongoType.getClass().isAssignableFrom( fieldValue.getClass() ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  protected static void processList( BasicDBList list, String path, String name, Map<String, MongoField> lookup ) {

    if ( list.size() == 0 ) {
      return; // can't infer anything about an empty list
    }

    String nonPrimitivePath = path + "[-]"; //$NON-NLS-1$
    String primitivePath = path;

    for ( int i = 0; i < list.size(); i++ ) {
      Object element = list.get( i );

      if ( element instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
      } else if ( element instanceof BasicDBList ) {
        processList( (BasicDBList) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
      } else {
        // some sort of primitive
        String finalPath = primitivePath + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        String finalName = name + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( element );
          newField.m_mongoType = element;
          newField.m_fieldName = finalPath;
          newField.m_fieldPath = finalName;
          newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          if ( !m.m_mongoType.getClass().isAssignableFrom( element.getClass() ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  protected static void postProcessPaths( Map<String, MongoField> fieldLookup, List<MongoField> discoveredFields,
      int numDocsProcessed ) {
    for ( String key : fieldLookup.keySet() ) {
      MongoField m = fieldLookup.get( key );
      m.m_occurenceFraction = "" + m.m_percentageOfSample + "/" //$NON-NLS-1$ //$NON-NLS-2$
          + numDocsProcessed;
      setMinArrayIndexes( m );

      // set field names to terminal part and copy any min:max array index
      // info
      if ( m.m_fieldName.contains( "[" ) && m.m_fieldName.contains( ":" ) ) { //$NON-NLS-1$ //$NON-NLS-2$
        m.m_arrayIndexInfo = m.m_fieldName;
      }
      if ( m.m_fieldName.indexOf( '.' ) >= 0 ) {
        m.m_fieldName = m.m_fieldName.substring( m.m_fieldName.lastIndexOf( '.' ) + 1, m.m_fieldName.length() );
      }

      if ( m.m_disparateTypes ) {
        // force type to string if we've seen this path more than once
        // with incompatible types
        m.m_kettleType = ValueMeta.getTypeDesc( ValueMeta.TYPE_STRING );
      }
      discoveredFields.add( m );
    }

    // check for name clashes
    Map<String, Integer> tempM = new HashMap<String, Integer>();
    for ( MongoField m : discoveredFields ) {
      if ( tempM.get( m.m_fieldName ) != null ) {
        Integer toUse = tempM.get( m.m_fieldName );
        String key = m.m_fieldName;
        m.m_fieldName = key + "_" + toUse; //$NON-NLS-1$
        toUse = new Integer( toUse.intValue() + 1 );
        tempM.put( key, toUse );
      } else {
        tempM.put( m.m_fieldName, 1 );
      }
    }
  }

  public static boolean discoverFields( final MongoDbInputMeta meta, final VariableSpace vars, final int docsToSample )
    throws KettleException {
    MongoClientWrapper clientWrapper = MongoClientWrapperFactory.createMongoClientWrapper( meta, vars, null );
    try {
      String db = vars.environmentSubstitute( meta.getDbName() );
      String collection = vars.environmentSubstitute( meta.getCollection() );
      String query = vars.environmentSubstitute( meta.getJsonQuery() );
      String fields = vars.environmentSubstitute( meta.getFieldsName() );
      int numDocsToSample = docsToSample;
      if ( numDocsToSample < 1 ) {
        numDocsToSample = 100; // default
      }
      List<MongoField> discoveredFields =
          clientWrapper.discoverFields( db, collection, query, fields, meta.getQueryIsPipeline(), numDocsToSample );

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if ( discoveredFields.size() > 0 ) {
        meta.setMongoFields( discoveredFields );

        return true;
      }
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( "Unable to discover fields from MongoDB", e );
      }
    } finally {
      clientWrapper.dispose();
    }

    return false;
  }

  protected static List<DBObject> jsonPipelineToDBObjectList( String jsonPipeline ) throws KettleException {
    List<DBObject> pipeline = new ArrayList<DBObject>();
    StringBuilder b = new StringBuilder( jsonPipeline.trim() );

    // extract the parts of the pipeline
    int bracketCount = -1;
    List<String> parts = new ArrayList<String>();
    int i = 0;
    while ( i < b.length() ) {
      if ( b.charAt( i ) == '{' ) {
        if ( bracketCount == -1 ) {
          // trim anything off before this point
          b.delete( 0, i );
          bracketCount = 0;
          i = 0;
        }
        bracketCount++;
      }
      if ( b.charAt( i ) == '}' ) {
        bracketCount--;
      }
      if ( bracketCount == 0 ) {
        String part = b.substring( 0, i + 1 );
        parts.add( part );
        bracketCount = -1;

        if ( i == b.length() - 1 ) {
          break;
        }
        b.delete( 0, i + 1 );
        i = 0;
      }

      i++;
    }

    for ( String p : parts ) {
      if ( !Const.isEmpty( p ) ) {
        DBObject o = (DBObject) JSON.parse( p );
        pipeline.add( o );
      }
    }

    if ( pipeline.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( MongoDbInputMeta.PKG,
          "MongoDbInput.ErrorMessage.UnableToParsePipelineOperators" ) ); //$NON-NLS-1$
    }

    return pipeline;
  }

  /**
   * Helper function that takes a list of indexed values and returns them as a String in comma-separated form.
   * 
   * @param indexedVals
   *          a list of indexed values
   * @return the list a String in comma-separated form
   */
  public static String indexedValsList( List<String> indexedVals ) {
    StringBuffer temp = new StringBuffer();

    for ( int i = 0; i < indexedVals.size(); i++ ) {
      temp.append( indexedVals.get( i ) );
      if ( i < indexedVals.size() - 1 ) {
        temp.append( "," ); //$NON-NLS-1$
      }
    }

    return temp.toString();
  }

  /**
   * Helper function that takes a comma-separated list in a String and returns a list.
   * 
   * @param indexedVals
   *          the String containing the lsit
   * @return a List containing the values
   */
  public static List<String> indexedValsList( String indexedVals ) {

    String[] parts = indexedVals.split( "," ); //$NON-NLS-1$
    List<String> list = new ArrayList<String>();
    for ( String s : parts ) {
      list.add( s.trim() );
    }

    return list;
  }
}
