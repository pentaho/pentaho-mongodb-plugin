/*!
 * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Data class for the MongoDbOutput step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MongoDbOutputData extends BaseStepData implements StepDataInterface {

  private static Class<?> PKG = MongoDbOutputMeta.class;

  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local"; //$NON-NLS-1$
  public static final String REPL_SET_COLLECTION = "system.replset"; //$NON-NLS-1$
  public static final String REPL_SET_SETTINGS = "settings"; //$NON-NLS-1$
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes"; //$NON-NLS-1$

  /**
   * Enum for the type of the top level object of the document structure
   */
  public enum MongoTopLevel {
    RECORD, ARRAY, INCONSISTENT;
  }

  /**
   * The output row format
   */
  protected RowMetaInterface m_outputRowMeta;

  /**
   * Main entry point to the mongo driver
   */
  protected MongoClientWrapper clientWrapper;

  /**
   * Collection object for the user-specified document collection
   */
  protected MongoCollectionWrapper m_collection;

  protected List<MongoDbOutputMeta.MongoField> m_userFields;

  /**
   * Map for grouping together $set operations that involve setting complex array-based objects. Key = dot path to array
   * name; value = DBObject specifying array to set to
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_setComplexArrays =
    new HashMap<String, List<MongoDbOutputMeta.MongoField>>();

  /**
   * Map for grouping together $push operations that involve complex objects. Use [] to indicate the start of the
   * complex object to push. Key - dot path to the array name to push to; value - DBObject specifying the complex object
   * to push
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_pushComplexStructures =
    new HashMap<String, List<MongoDbOutputMeta.MongoField>>();

  /**
   * all other modifier updates that involve primitive leaf fields
   */
  protected Map<String, Object[]> m_primitiveLeafModifiers = new LinkedHashMap<String, Object[]>();

  /**
   * True if the list of paths specifies an incoming Kettle field that contains a JSON doc that is intended to be
   * inserted as is (i.e. not added to a field in the document structure defined by the mongo paths)
   */
  protected boolean m_hasTopLevelJSONDocInsert = false;

  public static boolean scanForInsertTopLevelJSONDoc( List<MongoDbOutputMeta.MongoField> fieldDefs )
    throws KettleException {

    int countNonMatchFields = 0;
    boolean hasTopLevelJSONDocInsert = false;

    for ( MongoDbOutputMeta.MongoField f : fieldDefs ) {
      if ( f.m_JSON && !f.m_updateMatchField && Const.isEmpty( f.m_mongoDocPath )
        && !f.m_useIncomingFieldNameAsMongoFieldName ) {
        hasTopLevelJSONDocInsert = true;
      }

      if ( !f.m_updateMatchField ) {
        countNonMatchFields++;
      }
    }

    // Invalid path specification would be one where there is a top level
    // JSON doc to be inserted (as is) but other field paths have been defined.
    // TODO we could allow exactly one top level JSON doc and then have other
    // paths punch data into this document I guess (which is kind of the
    // opposite of the current functionality that allows the document to be
    // defined from the specified paths and then allows non top-level JSON docs
    // to punched into this structure)

    if ( hasTopLevelJSONDocInsert && countNonMatchFields > 1 ) {
      // TODO
      throw new KettleException( "Path specifications contains a top-level document in "
        + "JSON format to be inserted as is, but there are other insert paths "
        + "defined. When a top-level JSON document is to be inserted it must be "
        + "the only non-match field defined in the path specifications" );
    }

    return hasTopLevelJSONDocInsert;
  }

  /**
   * Set the field paths to use for creating the document structure
   *
   * @param fields the field paths to use
   */
  public void setMongoFields( List<MongoDbOutputMeta.MongoField> fields ) {
    // copy this list
    m_userFields = new ArrayList<MongoDbOutputMeta.MongoField>();

    for ( MongoDbOutputMeta.MongoField f : fields ) {
      m_userFields.add( f.copy() );
    }
  }

  /**
   * Gets the field paths to use for creating the document structure
   */
  public List<MongoDbOutputMeta.MongoField> getMongoFields() {
    return m_userFields;
  }

  /**
   * Initialize field paths
   *
   * @param vars variables to use
   * @throws KettleException if a problem occurs
   */
  public void init( VariableSpace vars ) throws KettleException {
    if ( m_userFields != null ) {
      for ( MongoDbOutputMeta.MongoField f : m_userFields ) {
        f.init( vars );
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
   * @param clientWrapper the connection to use
   */
  public void setConnection( MongoClientWrapper clientWrapper ) {
    this.clientWrapper = clientWrapper;
  }

  /**
   * Create a collection in the current database
   *
   * @param collectionName the name of the collection to create
   * @throws Exception if a problem occurs
   */
  public void createCollection( String db, String collectionName ) throws Exception {
    if ( clientWrapper == null ) {
      throw new Exception( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NoDatabaseSet" ) ); //$NON-NLS-1$
    }

    clientWrapper.createCollection( db, collectionName );
  }

  /**
   * Set the collection to use
   *
   * @param col the collection to use
   */
  public void setCollection( MongoCollectionWrapper col ) {
    m_collection = col;
  }

  /**
   * Get the collection in use
   *
   * @return the collection in use
   */
  public MongoCollectionWrapper getCollection() {
    return m_collection;
  }

  /**
   * Set the output row format
   *
   * @param outM the output row format
   */
  public void setOutputRowMeta( RowMetaInterface outM ) {
    m_outputRowMeta = outM;
  }

  /**
   * Get the output row format
   *
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Apply the supplied index operations to the collection. Indexes can be defined on one or more fields in the
   * document. Operation is either create or drop.
   *
   * @param indexes  a list of index operations
   * @param log      the logging object
   * @param truncate true if the collection was truncated in the current execution - in this case drop operations are
   *                 not necessary
   * @throws MongoException  if something goes wrong
   * @throws KettleException
   */
  public void applyIndexes( List<MongoDbOutputMeta.MongoIndex> indexes, LogChannelInterface log, boolean truncate )
    throws MongoException, KettleException, MongoDbException {

    for ( MongoDbOutputMeta.MongoIndex index : indexes ) {
      String[] indexParts = index.m_pathToFields.split( "," ); //$NON-NLS-1$
      BasicDBObject mongoIndex = new BasicDBObject();
      for ( String indexKey : indexParts ) {
        String[] nameAndDirection = indexKey.split( ":" ); //$NON-NLS-1$
        int direction = 1;
        if ( nameAndDirection.length == 2 ) {
          direction = Integer.parseInt( nameAndDirection[ 1 ].trim() );
        }
        String name = nameAndDirection[ 0 ];

        // strip off brackets to get actual object name if terminal object
        // is an array
        if ( name.indexOf( '[' ) > 0 ) {
          name = name.substring( name.indexOf( '[' ) + 1, name.length() );
        }

        mongoIndex.put( name, direction );
      }

      if ( index.m_drop ) {
        if ( truncate ) {
          log.logBasic(
            BaseMessages.getString( PKG, "MongoDbOutput.Messages.TruncateBeforeInsert", index ) ); //$NON-NLS-1$
        } else {
          m_collection.dropIndex( mongoIndex );
        }
        log.logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.DropIndex", index ) ); //$NON-NLS-1$
      } else {
        BasicDBObject options = new BasicDBObject();

        // create indexes in the background
        options.put( "background", true ); //$NON-NLS-1$
        options.put( "unique", index.m_unique ); //$NON-NLS-1$
        options.put( "sparse", index.m_sparse ); //$NON-NLS-1$
        m_collection.createIndex( mongoIndex, options );
        log.logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.CreateIndex", index ) ); //$NON-NLS-1$
      }
    }
  }

  /**
   * Get an object that encapsulates the fields and modifier operations to use for a modifier update.
   * <p/>
   * NOTE: that with modifier upserts the query conditions get created if the record does not exist (i.e. insert). This
   * is different than straight non- modifier upsert where the query conditions just locate a matching record (if any)
   * and then a complete object replacement is done. So for standard upsert it is necessary to duplicate the query
   * condition paths in order for these fields to be in the object that is inserted/updated.
   * <p/>
   * This also means that certain modifier upserts are not possible in the case of insert. E.g. here we are wanting to
   * test if the field "f1" in record "rec1" in the first element of array "two" is set to "george". If so, then we want
   * to push a new record to the end of the array; otherwise create a new document with the array containing just the
   * new record:
   * <p/>
   * <p/>
   * <pre>
   * db.collection.update({ "one.two.0.rec1.f1" : "george"},
   * { "$push" : { "one.two" : { "rec1" : { "f1" : "bob" , "f2" : "fred"}}}},
   * true)
   * </pre>
   * <p/>
   * This does not work and results in a "Cannot apply $push/$pushAll modifier to non-array" error if there is no match
   * (i.e. insert condition). This is because the query conditions get created as well as the modifier opps and,
   * furthermore, they get created first. Since mongo doesn't know whether ".0." indicates an array index or a field
   * name it defaults to creating a field with name "0". This means that "one.two" gets created as a record (not an
   * array) before the $push operation is executed. Hence the error.
   *
   * @param fieldDefs         the list of document field definitions
   * @param inputMeta         the input row format
   * @param row               the current incoming row
   * @param vars              environment variables
   * @param topLevelStructure the top level structure of the document
   * @return a DBObject encapsulating the update to make
   * @throws KettleException if a problem occurs
   */
  protected DBObject getModifierUpdateObject( List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
                                              Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure )
    throws KettleException, MongoDbException {

    boolean haveUpdateFields = false;
    boolean hasNonNullUpdateValues = false;

    // main update object, keyed by $ operator
    BasicDBObject updateObject = new BasicDBObject();

    m_setComplexArrays.clear();
    m_primitiveLeafModifiers.clear();
    m_pushComplexStructures.clear();

    // do we need to determine whether this will be an insert or an update?
    boolean checkForMatch = false;
    for ( MongoDbOutputMeta.MongoField field : fieldDefs ) {
      if ( !field.m_updateMatchField && ( field.m_modifierOperationApplyPolicy.equals( "Insert" )
        || field.m_modifierOperationApplyPolicy //$NON-NLS-1$
        .equals( "Update" ) ) ) { //$NON-NLS-1$
        checkForMatch = true;
        break;
      }
    }

    boolean isUpdate = false;
    if ( checkForMatch ) {
      DBObject query = getQueryObject( fieldDefs, inputMeta, row, vars, topLevelStructure );

      MongoCursorWrapper cursor = getCollection().find( query ).limit( 1 );
      if ( cursor.hasNext() ) {
        isUpdate = true;
      }
    }

    for ( MongoDbOutputMeta.MongoField field : fieldDefs ) {
      // skip query match fields
      if ( field.m_updateMatchField ) {
        continue;
      }

      String modifierUpdateOpp = vars.environmentSubstitute( field.m_modifierUpdateOperation );

      if ( !Const.isEmpty( modifierUpdateOpp ) && !modifierUpdateOpp.equals( "N/A" ) ) { //$NON-NLS-1$
        if ( checkForMatch ) {
          if ( isUpdate && field.m_modifierOperationApplyPolicy.equals( "Insert" ) ) { //$NON-NLS-1$
            continue; // don't apply this opp
          }

          if ( !isUpdate && field.m_modifierOperationApplyPolicy.equals( "Update" ) ) { //$NON-NLS-1$
            continue; // don't apply this opp
          }
        }

        haveUpdateFields = true;

        String incomingFieldName = vars.environmentSubstitute( field.m_incomingFieldName );
        int index = inputMeta.indexOfValue( incomingFieldName );
        ValueMetaInterface vm = inputMeta.getValueMeta( index );

        if ( !vm.isNull( row[ index ] ) || field.insertNull ) {
          hasNonNullUpdateValues = true;

          // modifier update objects have fields using "dot" notation to reach
          // into embedded documents
          String mongoPath = ( field.m_mongoDocPath != null ) ? field.m_mongoDocPath : ""; //$NON-NLS-1$
          String path = vars.environmentSubstitute( mongoPath );

          if ( path.endsWith( "]" ) && modifierUpdateOpp.equals( "$push" ) //$NON-NLS-1$ //$NON-NLS-2$
            && !field.m_useIncomingFieldNameAsMongoFieldName ) {

            // strip off the brackets as push appends to the end of the named
            // array
            path = path.substring( 0, path.indexOf( '[' ) );
          }

          boolean hasPath = !Const.isEmpty( path );
          path += ( ( field.m_useIncomingFieldNameAsMongoFieldName ) ? ( hasPath ? "." //$NON-NLS-1$
            + incomingFieldName : incomingFieldName ) : "" ); //$NON-NLS-1$

          // check for array creation
          if ( modifierUpdateOpp.equals( "$set" ) && path.indexOf( '[' ) > 0 ) { //$NON-NLS-1$
            String arrayPath = path.substring( 0, path.indexOf( '[' ) );
            String arraySpec = path.substring( path.indexOf( '[' ), path.length() );
            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.m_mongoDocPath = arraySpec;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.m_JSON = field.m_JSON;
            a.init( vars );
            List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays.get( arrayPath );
            if ( fds == null ) {
              fds = new ArrayList<MongoDbOutputMeta.MongoField>();
              m_setComplexArrays.put( arrayPath, fds );
            }
            fds.add( a );
          } else if ( modifierUpdateOpp.equals( "$push" ) && path.indexOf( '[' ) > 0 ) { //$NON-NLS-1$
            // we ignore any index that might have been specified as $push
            // always appends to the end of the array.
            String arrayPath = path.substring( 0, path.indexOf( '[' ) );
            String structureToPush = path.substring( path.indexOf( ']' ) + 1, path.length() );

            // check to see if we're pushing a record at this point in the path
            // or another array...
            if ( structureToPush.charAt( 0 ) == '.' ) {
              // skip the dot
              structureToPush = structureToPush.substring( 1, structureToPush.length() );
            }

            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.m_mongoDocPath = structureToPush;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.m_JSON = field.m_JSON;
            a.init( vars );
            List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures.get( arrayPath );
            if ( fds == null ) {
              fds = new ArrayList<MongoDbOutputMeta.MongoField>();
              m_pushComplexStructures.put( arrayPath, fds );
            }
            fds.add( a );
          } else {
            Object[] params = new Object[ 4 ];
            params[ 0 ] = modifierUpdateOpp;
            params[ 1 ] = index;
            params[ 2 ] = field.m_JSON;
            params[ 3 ] = field.insertNull;
            m_primitiveLeafModifiers.put( path, params );
          }
        }
      }
    }

    // do the array $sets
    for ( String path : m_setComplexArrays.keySet() ) {
      List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays.get( path );
      DBObject valueToSet = kettleRowToMongo( fds, inputMeta, row, vars, MongoTopLevel.ARRAY, false );

      DBObject fieldsToUpdateWithValues;

      if ( updateObject.get( "$set" ) != null ) { //$NON-NLS-1$
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get( "$set" ); //$NON-NLS-1$
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put( path, valueToSet );
      updateObject.put( "$set", fieldsToUpdateWithValues ); //$NON-NLS-1$
    }

    // now do the $push complex
    for ( String path : m_pushComplexStructures.keySet() ) {
      List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures.get( path );

      // check our top-level structure
      MongoTopLevel topLevel = MongoTopLevel.RECORD;
      if ( fds.get( 0 ).m_mongoDocPath.charAt( 0 ) == '[' ) {
        topLevel = MongoTopLevel.RECORD;
      }

      DBObject valueToSet = kettleRowToMongo( fds, inputMeta, row, vars, topLevel, false );

      DBObject fieldsToUpdateWithValues = null;

      if ( updateObject.get( "$push" ) != null ) { //$NON-NLS-1$
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get( "$push" ); //$NON-NLS-1$
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put( path, valueToSet );
      updateObject.put( "$push", fieldsToUpdateWithValues ); //$NON-NLS-1$
    }

    // do the modifiers that involve primitive field values
    for ( Map.Entry<String, Object[]> entry : m_primitiveLeafModifiers.entrySet() ) {
      String path = entry.getKey();
      Object[] params = entry.getValue();
      String modifierUpdateOpp = params[ 0 ].toString();
      int index = (Integer) params[ 1 ];
      boolean isJSON = (Boolean) params[ 2 ];
      boolean allowNull = (Boolean) params[ 3 ];
      ValueMetaInterface vm = inputMeta.getValueMeta( index );

      DBObject fieldsToUpdateWithValues = null;

      if ( updateObject.get( modifierUpdateOpp ) != null ) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get( modifierUpdateOpp );
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }
      setMongoValueFromKettleValue( fieldsToUpdateWithValues, path, vm, row[ index ], isJSON, allowNull );

      updateObject.put( modifierUpdateOpp, fieldsToUpdateWithValues );
    }

    if ( !haveUpdateFields ) {
      throw new KettleException( BaseMessages
        .getString( PKG, "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForModifierOpp" ) ); //$NON-NLS-1$
    }

    if ( !hasNonNullUpdateValues ) {
      return null;
    }

    return updateObject;
  }

  /**
   * Get an object that encapsulates the query to make for an update/upsert operation
   *
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row       the current incoming row
   * @param vars      environment variables
   * @return a DBObject encapsulating the query
   * @throws KettleException if something goes wrong
   */
  protected static DBObject getQueryObject( List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
                                            Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure )
    throws KettleException {
    BasicDBObject query = new BasicDBObject();

    boolean haveMatchFields = false;
    boolean hasNonNullMatchValues = false;

    for ( MongoDbOutputMeta.MongoField field : fieldDefs ) {
      if ( field.m_updateMatchField ) {
        haveMatchFields = true;
        String incomingFieldName = vars.environmentSubstitute( field.m_incomingFieldName );
        int index = inputMeta.indexOfValue( incomingFieldName );
        ValueMetaInterface vm = inputMeta.getValueMeta( index );

        // ignore null fields is not prohibited
        if ( vm.isNull( row[ index ] ) && !field.insertNull ) {
          continue;
        }

        hasNonNullMatchValues = true;

        if ( field.m_JSON && Const.isEmpty( field.m_mongoDocPath ) && !field.m_useIncomingFieldNameAsMongoFieldName ) {
          // We have a query based on a complete incoming JSON doc -
          // i.e. no field processing necessary

          if ( vm.isString() ) {
            String val = vm.getString( row[ index ] );
            query = (BasicDBObject) JSON.parse( val );
          } else {
            throw new KettleException(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.MatchFieldJSONButIncomingValueNotString" ) );
          }
          break;
        }

        // query objects have fields using "dot" notation to reach into embedded
        // documents
        String mongoPath = ( field.m_mongoDocPath != null ) ? field.m_mongoDocPath : ""; //$NON-NLS-1$
        String path = vars.environmentSubstitute( mongoPath );
        boolean hasPath = !Const.isEmpty( path );
        path += ( ( field.m_useIncomingFieldNameAsMongoFieldName ) ? ( hasPath ? "." //$NON-NLS-1$
          + incomingFieldName : incomingFieldName ) : "" ); //$NON-NLS-1$

        // post process arrays to fit the dot notation (if not already done
        // by the user)
        if ( path.indexOf( '[' ) > 0 ) {
          path = path.replace( "[", "." ).replace( "]", "" ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }

        setMongoValueFromKettleValue( query, path, vm, row[ index ], field.m_JSON, field.insertNull );
      }
    }

    if ( !haveMatchFields ) {
      throw new KettleException( BaseMessages
        .getString( PKG, "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForMatch" ) ); //$NON-NLS-1$
    }

    if ( !hasNonNullMatchValues ) {
      // indicates that we don't have anything to match with with respect to
      // this row
      return null;
    }

    return query;
  }

  /**
   * Converts a kettle row to a Mongo Object for inserting/updating
   *
   * @param fieldDefs                the document field definitions
   * @param inputMeta                the incoming row format
   * @param row                      the current incoming row
   * @param vars                     environment variables
   * @param topLevelStructure        the top level structure of the Mongo document
   * @param hasTopLevelJSONDocInsert true if the user-specified paths include a single incoming Kettle field value that
   *                                 contains a JSON document that is to be inserted as is
   * @return a DBObject encapsulating the document to insert/upsert or null if there are no non-null incoming fields
   * @throws KettleException if a problem occurs
   */
  protected static DBObject kettleRowToMongo( List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
                                              Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure,
                                              boolean hasTopLevelJSONDocInsert )
    throws KettleException {

    // the easy case
    if ( hasTopLevelJSONDocInsert ) {
      for ( MongoDbOutputMeta.MongoField f : fieldDefs ) {
        if ( f.m_JSON && Const.isEmpty( f.m_mongoDocPath ) && !f.m_useIncomingFieldNameAsMongoFieldName ) {
          String incomingFieldName = vars.environmentSubstitute( f.m_incomingFieldName );
          int index = inputMeta.indexOfValue( incomingFieldName );
          ValueMetaInterface vm = inputMeta.getValueMeta( index );
          if ( !vm.isNull( row[ index ] ) ) {
            String jsonDoc = vm.getString( row[ index ] );
            DBObject docToInsert = (DBObject) JSON.parse( jsonDoc );
            return docToInsert;
          } else {
            return null;
          }
        }
      }
    }

    DBObject root = null;
    if ( topLevelStructure == MongoTopLevel.RECORD ) {
      root = new BasicDBObject();
    } else if ( topLevelStructure == MongoTopLevel.ARRAY ) {
      root = new BasicDBList();
    }

    if ( vars == null ) {
      vars = new Variables();
    }

    boolean haveNonNullFields = false;
    for ( MongoDbOutputMeta.MongoField field : fieldDefs ) {
      DBObject current = root;

      field.reset();
      List<String> pathParts = field.m_tempPathList;
      String incomingFieldName = vars.environmentSubstitute( field.m_incomingFieldName );
      int index = inputMeta.indexOfValue( incomingFieldName );
      ValueMetaInterface vm = inputMeta.getValueMeta( index );

      Object lookup = getPathElementName( pathParts, current, field.m_useIncomingFieldNameAsMongoFieldName );
      do {
        // array?
        if ( lookup != null && lookup instanceof Integer ) {
          BasicDBList temp = (BasicDBList) current;
          if ( temp.get( lookup.toString() ) == null ) {
            if ( pathParts.size() == 0 && !field.m_useIncomingFieldNameAsMongoFieldName ) {
              // leaf - primitive element of the array (unless kettle field
              // value is JSON)
              boolean res =
                setMongoValueFromKettleValue( temp, lookup, vm, row[ index ], field.m_JSON, field.insertNull );
              haveNonNullFields = ( haveNonNullFields || res );
            } else {
              // must be a record here (since multi-dimensional array creation
              // is handled
              // in getPathElementName())

              // need to create this record/object
              BasicDBObject newRec = new BasicDBObject();
              temp.put( lookup.toString(), newRec );
              current = newRec;

              // end of the path?
              if ( pathParts.size() == 0 ) {
                if ( field.m_useIncomingFieldNameAsMongoFieldName ) {
                  boolean
                    res =
                    setMongoValueFromKettleValue( current, incomingFieldName, vm, row[ index ], field.m_JSON,
                      field.insertNull );
                  haveNonNullFields = ( haveNonNullFields || res );
                } else {
                  throw new KettleException( BaseMessages
                    .getString( PKG, "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath" ) ); //$NON-NLS-1$
                }
              }
            }
          } else {
            // existing element of the array
            current = (DBObject) temp.get( lookup.toString() );

            // no more path parts so we must be setting a field in an array
            // element
            // that is a record
            if ( pathParts == null || pathParts.size() == 0 ) {
              if ( current instanceof BasicDBObject ) {
                if ( field.m_useIncomingFieldNameAsMongoFieldName ) {
                  boolean
                    res =
                    setMongoValueFromKettleValue( current, incomingFieldName, vm, row[ index ], field.m_JSON,
                      field.insertNull );
                  haveNonNullFields = ( haveNonNullFields || res );
                } else {
                  throw new KettleException( BaseMessages
                    .getString( PKG, "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath" ) ); //$NON-NLS-1$
                }
              }
            }
          }
        } else {
          // record/object
          if ( lookup == null && pathParts.size() == 0 ) {
            if ( field.m_useIncomingFieldNameAsMongoFieldName ) {
              boolean res = setMongoValueFromKettleValue( current, incomingFieldName, vm, row[ index ], field.m_JSON,
                field.insertNull );
              haveNonNullFields = ( haveNonNullFields || res );
            } else {
              throw new KettleException( BaseMessages
                .getString( PKG, "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath" ) ); //$NON-NLS-1$
            }
          } else {
            if ( pathParts.size() == 0 ) {
              if ( !field.m_useIncomingFieldNameAsMongoFieldName ) {
                boolean res = setMongoValueFromKettleValue( current, lookup.toString(), vm, row[ index ], field.m_JSON,
                  field.insertNull );
                haveNonNullFields = ( haveNonNullFields || res );
              } else {
                current = (DBObject) current.get( lookup.toString() );
                boolean res = setMongoValueFromKettleValue( current, incomingFieldName, vm, row[ index ], field.m_JSON,
                  field.insertNull );
                haveNonNullFields = ( haveNonNullFields || res );
              }
            } else {
              current = (DBObject) current.get( lookup.toString() );
            }
          }
        }

        lookup = getPathElementName( pathParts, current, field.m_useIncomingFieldNameAsMongoFieldName );
      } while ( lookup != null );
    }

    if ( !haveNonNullFields ) {
      return null; // nothing has been set!
    }

    return root;
  }

  private static boolean setMongoValueFromKettleValue( DBObject mongoObject, Object lookup,
                                                       ValueMetaInterface kettleType, Object kettleValue,
                                                       boolean kettleValueIsJSON, boolean allowNull )
    throws KettleValueException {
    if ( kettleType.isNull( kettleValue ) ) {
      if ( allowNull ) {
        mongoObject.put( lookup.toString(), null );
        return true;
      } else {
        return false;
      }
    }

    if ( kettleType.isString() ) {
      String val = kettleType.getString( kettleValue );
      if ( kettleValueIsJSON ) {
        Object mongoO = JSON.parse( val );
        mongoObject.put( lookup.toString(), mongoO );
      } else {
        mongoObject.put( lookup.toString(), val );
      }
      return true;
    }
    if ( kettleType.isBoolean() ) {
      Boolean val = kettleType.getBoolean( kettleValue );
      mongoObject.put( lookup.toString(), val );
      return true;
    }
    if ( kettleType.isInteger() ) {
      Long val = kettleType.getInteger( kettleValue );
      mongoObject.put( lookup.toString(), val.longValue() );
      return true;
    }
    if ( kettleType.isDate() ) {
      Date val = kettleType.getDate( kettleValue );
      mongoObject.put( lookup.toString(), val );
      return true;
    }
    if ( kettleType.isNumber() ) {
      Double val = kettleType.getNumber( kettleValue );
      mongoObject.put( lookup.toString(), val.doubleValue() );
      return true;
    }
    if ( kettleType.isBigNumber() ) {
      // use string value - user can use Kettle to convert back
      String val = kettleType.getString( kettleValue );
      mongoObject.put( lookup.toString(), val );
      return true;
    }
    if ( kettleType.isBinary() ) {
      byte[] val = kettleType.getBinary( kettleValue );
      mongoObject.put( lookup.toString(), val );
      return true;
    }
    if ( kettleType.isSerializableType() ) {
      throw new KettleValueException(
        BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.CantStoreKettleSerializableVals" ) ); //$NON-NLS-1$
    }

    return false;
  }

  private static Object getPathElementName( List<String> pathParts, DBObject current, boolean incomingAsFieldName )
    throws KettleException {

    if ( pathParts == null || pathParts.size() == 0 ) {
      return null;
    }

    String part = pathParts.get( 0 );
    if ( part.startsWith( "[" ) ) { //$NON-NLS-1$
      String index = part.substring( 1, part.indexOf( ']' ) ).trim();
      part = part.substring( part.indexOf( ']' ) + 1 ).trim();
      if ( part.length() > 0 ) {
        // any remaining characters must indicate a multi-dimensional array
        pathParts.set( 0, part );

        // does this next array exist?
        if ( current.get( index ) == null ) {
          BasicDBList newArr = new BasicDBList();
          current.put( index, newArr );
        }
      } else {
        // remove - we're finished with this part
        pathParts.remove( 0 );
      }
      return new Integer( index );
    } else if ( part.endsWith( "]" ) ) { //$NON-NLS-1$
      String fieldName = part.substring( 0, part.indexOf( '[' ) );
      Object mongoField = current.get( fieldName );
      if ( mongoField == null ) {
        // create this field
        BasicDBList newField = new BasicDBList();
        current.put( fieldName, newField );
      } else {
        // check type - should be an array
        if ( !( mongoField instanceof BasicDBList ) ) {
          throw new KettleException( BaseMessages
            .getString( PKG, "MongoDbOutput.Messages.Error.FieldExistsButIsntAnArray", part ) ); //$NON-NLS-1$
        }
      }
      part = part.substring( part.indexOf( '[' ) );
      pathParts.set( 0, part );

      return fieldName;
    }

    // otherwise this path part is a record (object) or possibly a leaf (if we
    // are not
    // using the incoming kettle field name as the mongo field name)
    Object mongoField = current.get( part );
    if ( mongoField == null ) {
      if ( incomingAsFieldName || pathParts.size() > 1 ) {
        // create this field
        BasicDBObject newField = new BasicDBObject();
        current.put( part, newField );
      }
    } else {
      // check type = should be a record (object)
      if ( !( mongoField instanceof BasicDBObject ) && pathParts.size() > 1 ) {
        throw new KettleException( BaseMessages
          .getString( PKG, "MongoDbOutput.Messages.Error.FieldExistsButIsntARecord", part ) ); //$NON-NLS-1$
      }
    }
    pathParts.remove( 0 );
    return part;
  }

  /**
   * Determines the top level structure of the outgoing Mongo document from the user-specified field paths. This can be
   * either RECORD ( for a top level structure that is an object), ARRAY or INCONSISTENT (if the user has some field
   * paths that start with an array and some that start with an object).
   *
   * @param fieldDefs the list of document field paths
   * @param vars      environment variables
   * @return the top level structure
   */
  protected static MongoTopLevel checkTopLevelConsistency( List<MongoDbOutputMeta.MongoField> fieldDefs,
                                                           VariableSpace vars ) throws KettleException {

    if ( fieldDefs == null || fieldDefs.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NoMongoPathsDefined" ) );
    }

    int numRecords = 0;
    int numArrays = 0;

    for ( MongoDbOutputMeta.MongoField field : fieldDefs ) {
      String mongoPath = vars.environmentSubstitute( field.m_mongoDocPath );

      if ( Const.isEmpty( mongoPath ) ) {
        numRecords++;
      } else if ( mongoPath.startsWith( "[" ) ) { //$NON-NLS-1$
        numArrays++;
      } else {
        numRecords++;
      }
    }

    if ( numRecords < fieldDefs.size() && numArrays < fieldDefs.size() ) {
      return MongoTopLevel.INCONSISTENT;
    }

    if ( numRecords > 0 ) {
      return MongoTopLevel.RECORD;
    }

    return MongoTopLevel.ARRAY;
  }

}
