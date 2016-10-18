/*!
 * Copyright 2010 - 2016 Pentaho Corporation.  All rights reserved.
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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Class providing an output step for writing data to a MongoDB collection. Supports insert, truncate, upsert,
 * multi-update (update all matching docs) and modifier update (update only certain fields) operations. Can also create
 * and drop indexes based on one or more fields.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
@Step( id = "MongoDbOutput", image = "MongoDB.svg", name = "MongoDB Output",
    description = "Writes to a Mongo DB collection", documentationUrl = "http://wiki.pentaho.com/display/EAI/MongoDB+Output", categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "MongoDbOutput.Injection.", groups = { "FIELDS", "INDEXES" } )
public class MongoDbOutputMeta extends MongoDbMeta implements StepMetaInterface {

  private static Class<?> PKG = MongoDbOutputMeta.class; // for i18n purposes

  /**
   * Class encapsulating paths to document fields
   * 
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  public static class MongoField {

    /** Incoming Kettle field name */
    @Injection( name = "INCOMING_FIELD_NAME", group = "FIELDS" )
    public String m_incomingFieldName = ""; //$NON-NLS-1$

    /**
     * Dot separated path to the corresponding mongo field
     */
    @Injection( name = "MONGO_DOCUMENT_PATH", group = "FIELDS" )
    public String m_mongoDocPath = ""; //$NON-NLS-1$

    protected List<String> m_pathList;
    protected List<String> m_tempPathList;

    /**
     * Whether to use the incoming field name as the mongo field key name. If false then the user must supply the
     * terminating field/key name.
     */
    @Injection( name = "INCOMING_AS_MONGO", group = "FIELDS" )
    public boolean m_useIncomingFieldNameAsMongoFieldName;

    /** Whether this field is used in the query for an update operation */
    @Injection( name = "UPDATE_MATCH_FIELD", group = "FIELDS" )
    public boolean m_updateMatchField;

    /**
     * Ignored if not doing a modifier update since all mongo paths are involved in a standard upsert. If null/empty
     * then this field is not being updated in the modifier update case.
     * 
     * $set $inc $push - append value to array (or set to [value] if field doesn't exist)
     * 
     * (support any others?)
     */
    @Injection( name = "MODIFIER_OPERATION", group = "FIELDS" )
    public String m_modifierUpdateOperation = "N/A"; //$NON-NLS-1$

    /**
     * If a modifier opp, whether to apply on insert, update or both. Insert or update require knowing whether matching
     * record(s) exist, so require the overhead of a find().limit(1) query for each row. The motivation for this is to
     * allow a single document's structure to be created and developed over multiple kettle rows. E.g. say a document is
     * to contain an array of records where each record in the array itself has a field with is an array. The $push
     * operator specifies the terminal array to add an element to via the dot notation. This terminal array will be
     * created if it does not already exist in the target document; however, it will not create the intermediate array
     * (i.e. the first array in the path). To do this requires a $set operation that is executed only on insert (i.e. if
     * the target document does not exist). Whereas the $push operation should occur only on updates. A single operation
     * can't combine these two as it will result in a conflict (since they operate on the same array).
     */
    @Injection( name = "MODIFIER_POLICY", group = "FIELDS" )
    public String m_modifierOperationApplyPolicy = "Insert&Update"; //$NON-NLS-1$

    /**
     * This flag determines the strategy of handling {@code null} values. By default, {@code null}s are ignored. If
     * this flag is set to {@code true}, the corresponding output value is sent as {@code null}.
     * <br/>
     * Note: {@code null} and {@code undefined} are different values in Mongo!
     */
    @Injection( name = "INSERT_NULL", group = "FIELDS" )
    public boolean insertNull = false;

    /**
     * If true, then the incoming Kettle field value for this mongo field is expected to be of type String and hold a
     * Mongo doc fragment in JSON format. The doc fragment will be converted into BSON and added into the overall
     * document at the point specified by this MongoField's path
     */
    @Injection( name = "JSON", group = "FIELDS" )
    public boolean m_JSON = false;

    public MongoField copy() {
      MongoField newF = new MongoField();
      newF.m_incomingFieldName = m_incomingFieldName;
      newF.m_mongoDocPath = m_mongoDocPath;
      newF.m_useIncomingFieldNameAsMongoFieldName = m_useIncomingFieldNameAsMongoFieldName;
      newF.m_updateMatchField = m_updateMatchField;
      newF.m_modifierUpdateOperation = m_modifierUpdateOperation;
      newF.m_modifierOperationApplyPolicy = m_modifierOperationApplyPolicy;
      newF.m_JSON = m_JSON;
      newF.insertNull = insertNull;

      return newF;
    }

    public void init( VariableSpace vars ) {
      String path = vars.environmentSubstitute( m_mongoDocPath );
      m_pathList = new ArrayList<String>();

      if ( !Const.isEmpty( path ) ) {
        String[] parts = path.split( "\\." ); //$NON-NLS-1$
        for ( String p : parts ) {
          m_pathList.add( p );
        }
      }
      m_tempPathList = new ArrayList<String>( m_pathList );
    }

    public void reset() {
      if ( m_tempPathList != null && m_tempPathList.size() > 0 ) {
        m_tempPathList.clear();
      }
      if ( m_tempPathList != null ) {
        m_tempPathList.addAll( m_pathList );
      }
    }
  }

  /**
   * Class encapsulating index definitions
   * 
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  public static class MongoIndex {

    /**
     * Dot notation for accessing a fields - e.g. person.address.street. Can also specify entire embedded documents as
     * an index (rather than a primitive key) - e.g. person.address.
     * 
     * Multiple fields are comma-separated followed by an optional "direction" indicator for the index (1 or -1). If
     * omitted, direction is assumed to be 1.
     */
    @Injection( name = "INDEX_FIELD", group = "INDEXES" )
    public String m_pathToFields = ""; //$NON-NLS-1$

    /** whether to drop this index - default is create */
    @Injection( name = "DROP", group = "INDEXES" )
    public boolean m_drop;

    // other options unique, sparse
    @Injection( name = "UNIQUE", group = "INDEXES" )
    public boolean m_unique;
    @Injection( name = "SPARSE", group = "INDEXES" )
    public boolean m_sparse;

    @Override
    public String toString() {
      StringBuffer buff = new StringBuffer();
      buff.append( m_pathToFields + " (unique = " //$NON-NLS-1$
          + new Boolean( m_unique ).toString() + " sparse = " //$NON-NLS-1$
          + new Boolean( m_sparse ).toString() + ")" ); //$NON-NLS-1$

      return buff.toString();
    }
  }

  /** Whether to truncate the collection */
  @Injection( name = "TRUNCATE" )
  protected boolean m_truncate;

  /** True if updates (rather than inserts) are to be performed */
  @Injection( name = "UPDATE" )
  protected boolean m_update;

  /** True if upserts are to be performed */
  @Injection( name = "UPSERT" )
  protected boolean m_upsert;

  /**
   * whether to update all records that match during an upsert or just the first
   */
  @Injection( name = "MULTI" )
  protected boolean m_multi;

  /**
   * Modifier update involves updating only some fields and is efficient because of low network overhead. Is also
   * particularly efficient for $incr operations since the queried object does not have to be returned in order to
   * increment the field and then saved again.
   * 
   * If modifier update is false, then the standard update/insert operation is performed which involves replacing the
   * matched object with a new object involving all the user-defined mongo paths
   */
  @Injection( name = "MODIFIER_UPDATE" )
  protected boolean m_modifierUpdate;

  /** The batch size for inserts */
  @Injection( name = "BATCH_INSERT_SIZE" )
  protected String m_batchInsertSize = "100"; //$NON-NLS-1$

  /** The list of paths to document fields for incoming kettle values */
  @InjectionDeep
  protected List<MongoField> m_mongoFields;

  /** The list of index definitions (if any) */
  @InjectionDeep
  protected List<MongoIndex> m_mongoIndexes;

  public static final int RETRIES = 5;
  public static final int RETRY_DELAY = 10; // seconds

  @Injection( name = "RETRY_NUMBER" )
  private String m_writeRetries = "" + RETRIES; //$NON-NLS-1$
  @Injection( name = "RETRY_DELAY" )
  private String m_writeRetryDelay = "" + RETRY_DELAY; // seconds //$NON-NLS-1$

  @Override
  public void setDefault() {
    setHostnames( "localhost" ); //$NON-NLS-1$
    setPort( "27017" ); //$NON-NLS-1$
    setCollection( "" ); //$NON-NLS-1$
    setDbName( "" ); //$NON-NLS-1$
    setAuthenticationMechanism( "" );
    m_upsert = false;
    m_modifierUpdate = false;
    m_truncate = false;
    m_batchInsertSize = "100"; //$NON-NLS-1$
    setWriteConcern( "" );
    setWTimeout( "" );
    setJournal( false );
  }

  /**
   * Set the list of document paths
   * 
   * @param mongoFields
   *          the list of document paths
   */
  public void setMongoFields( List<MongoField> mongoFields ) {
    m_mongoFields = mongoFields;
  }

  /**
   * Get the list of document paths
   * 
   * @return the list of document paths
   */
  public List<MongoField> getMongoFields() {
    return m_mongoFields;
  }

  /**
   * Set the list of document indexes for creation/dropping
   * 
   * @param mongoIndexes
   *          the list of indexes
   */
  public void setMongoIndexes( List<MongoIndex> mongoIndexes ) {
    m_mongoIndexes = mongoIndexes;
  }

  /**
   * Get the list of document indexes for creation/dropping
   * 
   * @return the list of indexes
   */
  public List<MongoIndex> getMongoIndexes() {
    return m_mongoIndexes;
  }

  /**
   * 
   * @param r
   *          the number of retry attempts to make
   */
  public void setWriteRetries( String r ) {
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
   * @param d
   *          the delay in seconds between retry attempts
   */
  public void setWriteRetryDelay( String d ) {
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

  /**
   * Set whether updates (rather than inserts) are to be performed
   * 
   * @param update
   */
  public void setUpdate( boolean update ) {
    m_update = update;
  }

  /**
   * Get whether updates (rather than inserts) are to be performed
   * 
   * @return true if updates are to be performed
   */
  public boolean getUpdate() {
    return m_update;
  }

  /**
   * Set whether to upsert rather than update
   * 
   * @param upsert
   *          true if we'll upsert rather than update
   */
  public void setUpsert( boolean upsert ) {
    m_upsert = upsert;
  }

  /**
   * Get whether to upsert rather than update
   * 
   * @return true if we'll upsert rather than update
   */
  public boolean getUpsert() {
    return m_upsert;
  }

  /**
   * Set whether the upsert should update all matching records rather than just the first.
   * 
   * @param multi
   *          true if all matching records get updated when each row is upserted
   */
  public void setMulti( boolean multi ) {
    m_multi = multi;
  }

  /**
   * Get whether the upsert should update all matching records rather than just the first.
   * 
   * @return true if all matching records get updated when each row is upserted
   */
  public boolean getMulti() {
    return m_multi;
  }

  /**
   * Set whether the upsert operation is a modifier update - i.e where only specified fields in each document get
   * modified rather than a whole document replace.
   * 
   * @param u
   *          true if the upsert operation is to be a modifier update
   */
  public void setModifierUpdate( boolean u ) {
    m_modifierUpdate = u;
  }

  /**
   * Get whether the upsert operation is a modifier update - i.e where only specified fields in each document get
   * modified rather than a whole document replace.
   * 
   * @return true if the upsert operation is to be a modifier update
   */
  public boolean getModifierUpdate() {
    return m_modifierUpdate;
  }

  /**
   * Set whether to truncate the collection before inserting
   * 
   * @param truncate
   *          true if the all records in the collection are to be deleted
   */
  public void setTruncate( boolean truncate ) {
    m_truncate = truncate;
  }

  /**
   * Get whether to truncate the collection before inserting
   * 
   * @return true if the all records in the collection are to be deleted
   */
  public boolean getTruncate() {
    return m_truncate;
  }

  /**
   * Get the batch insert size
   * 
   * @return the batch insert size
   */
  public String getBatchInsertSize() {
    return m_batchInsertSize;
  }

  /**
   * Set the batch insert size
   * 
   * @param size
   *          the batch insert size
   */
  public void setBatchInsertSize( String size ) {
    m_batchInsertSize = size;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {

    CheckResult cr;

    if ( ( prev == null ) || ( prev.size() == 0 ) ) {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
              "MongoDbOutput.Messages.Error.NotReceivingFieldsFromPreviousSteps" ), //$NON-NLS-1$
              stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "MongoDbOutput.Messages.ReceivingFields", prev.size() ), stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "MongoDbOutput.Messages.ReceivingInfo" ), stepMeta ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "MongoDbOutput.Messages.Error.NoInputReceivedFromOtherSteps" ), //$NON-NLS-1$
              stepMeta );
      remarks.add( cr );
    }

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans ) {

    return new MongoDbOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MongoDbOutputData();
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer();

    if ( !Const.isEmpty( getHostnames() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_host", getHostnames() ) ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( getPort() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_port", getPort() ) ); //$NON-NLS-1$
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( "use_all_replica_members", getUseAllReplicaSetMembers() ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( !Const.isEmpty( getAuthenticationDatabaseName() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
              XMLHandler.addTagValue( "mongo_auth_database", getAuthenticationDatabaseName() ) ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( getAuthenticationUser() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_user", getAuthenticationUser() ) ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( getAuthenticationPassword() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_password", //$NON-NLS-1$
              Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) ) );
    }
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_mech", getAuthenticationMechanism() ) );

    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_kerberos", getUseKerberosAuthentication() ) ); //$NON-NLS-1$

    if ( !Const.isEmpty( getDbName() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_db", getDbName() ) ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( getCollection() ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "mongo_collection", getCollection() ) ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( m_batchInsertSize ) ) {
      retval.append( "\n    " ).append( //$NON-NLS-1$
          XMLHandler.addTagValue( "batch_insert_size", m_batchInsertSize ) ); //$NON-NLS-1$
    }

    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "connect_timeout", getConnectTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "socket_timeout", getSocketTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "read_preference", getReadPreference() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "write_concern", getWriteConcern() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "w_timeout", getWTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "journaled_writes", getJournal() ) ); //$NON-NLS-1$

    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "truncate", m_truncate ) ); //$NON-NLS-1$
    retval.append( "\n    " ).append( XMLHandler.addTagValue( "update", m_update ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "\n    " ).append( XMLHandler.addTagValue( "upsert", m_upsert ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "\n    " ).append( XMLHandler.addTagValue( "multi", m_multi ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "\n    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "modifier_update", m_modifierUpdate ) ); //$NON-NLS-1$

    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "write_retries", m_writeRetries ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "write_retry_delay", m_writeRetryDelay ) ); //$NON-NLS-1$

    if ( m_mongoFields != null && m_mongoFields.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$

      for ( MongoField field : m_mongoFields ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$

        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "incoming_field_name", //$NON-NLS-1$
                field.m_incomingFieldName ) );
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "mongo_doc_path", field.m_mongoDocPath ) ); //$NON-NLS-1$
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "use_incoming_field_name_as_mongo_field_name", //$NON-NLS-1$
                field.m_useIncomingFieldNameAsMongoFieldName ) );
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "update_match_field", //$NON-NLS-1$
                field.m_updateMatchField ) );
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "modifier_update_operation", //$NON-NLS-1$
                field.m_modifierUpdateOperation ) );
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "modifier_policy", //$NON-NLS-1$
                field.m_modifierOperationApplyPolicy ) );
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "json_field", field.m_JSON ) ); //$NON-NLS-1$
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "allow_null", field.insertNull ) ); //$NON-NLS-1$

        retval.append( "\n      " ).append( XMLHandler.closeTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    if ( m_mongoIndexes != null && m_mongoIndexes.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "mongo_indexes" ) ); //$NON-NLS-1$ //$NON-NLS-2$

      for ( MongoIndex index : m_mongoIndexes ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "mongo_index" ) ); //$NON-NLS-1$ //$NON-NLS-2$

        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "path_to_fields", index.m_pathToFields ) ); //$NON-NLS-1$
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "drop", index.m_drop ) ); //$NON-NLS-1$
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "unique", index.m_unique ) ); //$NON-NLS-1$
        retval.append( "\n         " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "sparse", index.m_sparse ) ); //$NON-NLS-1$

        retval.append( "\n      " ).append( XMLHandler.closeTag( "mongo_index" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "mongo_indexes" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    setHostnames( XMLHandler.getTagValue( stepnode, "mongo_host" ) ); //$NON-NLS-1$
    setPort( XMLHandler.getTagValue( stepnode, "mongo_port" ) ); //$NON-NLS-1$
    setAuthenticationDatabaseName( XMLHandler.getTagValue( stepnode, "mongo_auth_database" ) ); //$NON-NLS-1$
    setAuthenticationUser( XMLHandler.getTagValue( stepnode, "mongo_user" ) ); //$NON-NLS-1$
    setAuthenticationPassword( XMLHandler.getTagValue( stepnode, "mongo_password" ) ); //$NON-NLS-1$
    if ( !Const.isEmpty( getAuthenticationPassword() ) ) {
      setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( getAuthenticationPassword() ) );
    }

    setAuthenticationMechanism( XMLHandler.getTagValue( stepnode, "auth_mech" ) );

    setUseKerberosAuthentication( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "auth_kerberos" ) ) ); //$NON-NLS-1$
    setDbName( XMLHandler.getTagValue( stepnode, "mongo_db" ) ); //$NON-NLS-1$
    setCollection( XMLHandler.getTagValue( stepnode, "mongo_collection" ) ); //$NON-NLS-1$
    m_batchInsertSize = XMLHandler.getTagValue( stepnode, "batch_insert_size" ); //$NON-NLS-1$

    setConnectTimeout( XMLHandler.getTagValue( stepnode, "connect_timeout" ) ); //$NON-NLS-1$
    setSocketTimeout( XMLHandler.getTagValue( stepnode, "socket_timeout" ) ); //$NON-NLS-1$
    setReadPreference( XMLHandler.getTagValue( stepnode, "read_preference" ) ); //$NON-NLS-1$
    setWriteConcern( XMLHandler.getTagValue( stepnode, "write_concern" ) ); //$NON-NLS-1$
    setWTimeout( XMLHandler.getTagValue( stepnode, "w_timeout" ) ); //$NON-NLS-1$
    String journaled = XMLHandler.getTagValue( stepnode, "journaled_writes" ); //$NON-NLS-1$
    if ( !Const.isEmpty( journaled ) ) {
      setJournal( journaled.equalsIgnoreCase( "Y" ) ); //$NON-NLS-1$
    }

    m_truncate = XMLHandler.getTagValue( stepnode, "truncate" ).equalsIgnoreCase( //$NON-NLS-1$
        "Y" ); //$NON-NLS-1$

    // for backwards compatibility with older ktrs
    String update = XMLHandler.getTagValue( stepnode, "update" );
    if ( !Const.isEmpty( update ) ) {
      m_update = update.equalsIgnoreCase( "Y" ); //$NON-NLS-1$ 
    }

    m_upsert = XMLHandler.getTagValue( stepnode, "upsert" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$
    m_multi = XMLHandler.getTagValue( stepnode, "multi" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$
    m_modifierUpdate = XMLHandler.getTagValue( stepnode, "modifier_update" ) //$NON-NLS-1$
        .equalsIgnoreCase( "Y" ); //$NON-NLS-1$

    // for backwards compatibility with older ktrs (to maintain correct
    // operation)
    if ( m_upsert || m_multi ) {
      m_update = true;
    }

    setUseAllReplicaSetMembers( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_all_replica_members" ) ) ); //$NON-NLS-1$ //$NON-NLS-2$

    String writeRetries = XMLHandler.getTagValue( stepnode, "write_retries" ); //$NON-NLS-1$
    if ( !Const.isEmpty( writeRetries ) ) {
      m_writeRetries = writeRetries;
    }
    String writeRetryDelay = XMLHandler.getTagValue( stepnode, "write_retry_delay" ); //$NON-NLS-1$
    if ( !Const.isEmpty( writeRetryDelay ) ) {
      m_writeRetryDelay = writeRetryDelay;
    }

    Node fields = XMLHandler.getSubNode( stepnode, "mongo_fields" ); //$NON-NLS-1$
    if ( fields != null && XMLHandler.countNodes( fields, "mongo_field" ) > 0 ) { //$NON-NLS-1$
      int nrfields = XMLHandler.countNodes( fields, "mongo_field" ); //$NON-NLS-1$
      m_mongoFields = new ArrayList<MongoField>();

      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( fields, "mongo_field", i ); //$NON-NLS-1$

        MongoField newField = new MongoField();
        newField.m_incomingFieldName = XMLHandler.getTagValue( fieldNode, "incoming_field_name" ); //$NON-NLS-1$
        newField.m_mongoDocPath = XMLHandler.getTagValue( fieldNode, "mongo_doc_path" ); //$NON-NLS-1$
        newField.m_useIncomingFieldNameAsMongoFieldName =
            XMLHandler.getTagValue( fieldNode, "use_incoming_field_name_as_mongo_field_name" ) //$NON-NLS-1$
                .equalsIgnoreCase( "Y" ); //$NON-NLS-1$
        newField.m_updateMatchField = XMLHandler.getTagValue( fieldNode, "update_match_field" ).equalsIgnoreCase( "Y" ); //$NON-NLS-1$ //$NON-NLS-2$

        newField.m_modifierUpdateOperation = XMLHandler.getTagValue( fieldNode, "modifier_update_operation" ); //$NON-NLS-1$
        String policy = XMLHandler.getTagValue( fieldNode, "modifier_policy" ); //$NON-NLS-1$
        if ( !Const.isEmpty( policy ) ) {
          newField.m_modifierOperationApplyPolicy = policy;
        }
        String jsonField = XMLHandler.getTagValue( fieldNode, "json_field" ); //$NON-NLS-1$
        if ( !Const.isEmpty( jsonField ) ) {
          newField.m_JSON = jsonField.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
        }
        String allowNull = XMLHandler.getTagValue( fieldNode, "allow_null" ); //$NON-NLS-1$
        newField.insertNull = "Y".equalsIgnoreCase( allowNull );

        m_mongoFields.add( newField );
      }
    }

    fields = XMLHandler.getSubNode( stepnode, "mongo_indexes" ); //$NON-NLS-1$
    if ( fields != null && XMLHandler.countNodes( fields, "mongo_index" ) > 0 ) { //$NON-NLS-1$
      int nrfields = XMLHandler.countNodes( fields, "mongo_index" ); //$NON-NLS-1$

      m_mongoIndexes = new ArrayList<MongoIndex>();

      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( fields, "mongo_index", i ); //$NON-NLS-1$

        MongoIndex newIndex = new MongoIndex();

        newIndex.m_pathToFields = XMLHandler.getTagValue( fieldNode, "path_to_fields" ); //$NON-NLS-1$
        newIndex.m_drop = XMLHandler.getTagValue( fieldNode, "drop" ) //$NON-NLS-1$
            .equalsIgnoreCase( "Y" ); //$NON-NLS-1$
        newIndex.m_unique = XMLHandler.getTagValue( fieldNode, "unique" ) //$NON-NLS-1$
            .equalsIgnoreCase( "Y" ); //$NON-NLS-1$
        newIndex.m_sparse = XMLHandler.getTagValue( fieldNode, "sparse" ) //$NON-NLS-1$
            .equalsIgnoreCase( "Y" ); //$NON-NLS-1$

        m_mongoIndexes.add( newIndex );
      }
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    setHostnames( rep.getStepAttributeString( id_step, 0, "mongo_host" ) ); //$NON-NLS-1$
    setPort( rep.getStepAttributeString( id_step, 0, "mongo_port" ) ); //$NON-NLS-1$
    setUseAllReplicaSetMembers( rep.getStepAttributeBoolean( id_step, 0, "use_all_replica_members" ) ); //$NON-NLS-1$
    setAuthenticationDatabaseName( rep.getStepAttributeString( id_step, 0, "mongo_auth_database" ) ); //$NON-NLS-1$
    setAuthenticationUser( rep.getStepAttributeString( id_step, 0, "mongo_user" ) ); //$NON-NLS-1$
    setAuthenticationPassword( rep.getStepAttributeString( id_step, 0, "mongo_password" ) ); //$NON-NLS-1$
    if ( !Const.isEmpty( getAuthenticationPassword() ) ) {
      setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( getAuthenticationPassword() ) );
    }
    setAuthenticationMechanism( rep.getStepAttributeString( id_step, "auth_mech" ) );
    setUseKerberosAuthentication( rep.getStepAttributeBoolean( id_step, "auth_kerberos" ) ); //$NON-NLS-1$
    setDbName( rep.getStepAttributeString( id_step, 0, "mongo_db" ) ); //$NON-NLS-1$
    setCollection( rep.getStepAttributeString( id_step, 0, "mongo_collection" ) ); //$NON-NLS-1$
    m_batchInsertSize = rep.getStepAttributeString( id_step, 0, "batch_insert_size" ); //$NON-NLS-1$

    setConnectTimeout( rep.getStepAttributeString( id_step, "connect_timeout" ) ); //$NON-NLS-1$
    setSocketTimeout( rep.getStepAttributeString( id_step, "socket_timeout" ) ); //$NON-NLS-1$
    setReadPreference( rep.getStepAttributeString( id_step, "read_preference" ) ); //$NON-NLS-1$
    setWriteConcern( rep.getStepAttributeString( id_step, "write_concern" ) ); //$NON-NLS-1$
    setWTimeout( rep.getStepAttributeString( id_step, "w_timeout" ) ); //$NON-NLS-1$
    setJournal( rep.getStepAttributeBoolean( id_step, 0, "journaled_writes" ) ); //$NON-NLS-1$

    m_truncate = rep.getStepAttributeBoolean( id_step, 0, "truncate" ); //$NON-NLS-1$
    m_update = rep.getStepAttributeBoolean( id_step, 0, "update" ); //$NON-NLS-1$
    m_upsert = rep.getStepAttributeBoolean( id_step, 0, "upsert" ); //$NON-NLS-1$
    m_multi = rep.getStepAttributeBoolean( id_step, 0, "multi" ); //$NON-NLS-1$
    m_modifierUpdate = rep.getStepAttributeBoolean( id_step, 0, "modifier_update" ); //$NON-NLS-1$

    if ( m_upsert || m_multi ) {
      m_update = true;
    }

    int nrfields = rep.countNrStepAttributes( id_step, "incoming_field_name" ); //$NON-NLS-1$

    String writeRetries = rep.getStepAttributeString( id_step, "write_retries" ); //$NON-NLS-1$
    if ( !Const.isEmpty( writeRetries ) ) {
      m_writeRetries = writeRetries;
    }
    String writeRetryDelay = rep.getStepAttributeString( id_step, "write_retry_delay" ); //$NON-NLS-1$
    if ( !Const.isEmpty( writeRetryDelay ) ) {
      m_writeRetryDelay = writeRetryDelay;
    }

    if ( nrfields > 0 ) {
      m_mongoFields = new ArrayList<MongoField>();

      for ( int i = 0; i < nrfields; i++ ) {
        MongoField newField = new MongoField();

        newField.m_incomingFieldName = rep.getStepAttributeString( id_step, i, "incoming_field_name" ); //$NON-NLS-1$
        newField.m_mongoDocPath = rep.getStepAttributeString( id_step, i, "mongo_doc_path" ); //$NON-NLS-1$

        newField.m_useIncomingFieldNameAsMongoFieldName =
            rep.getStepAttributeBoolean( id_step, i, "use_incoming_field_name_as_mongo_field_name" ); //$NON-NLS-1$
        newField.m_updateMatchField = rep.getStepAttributeBoolean( id_step, i, "update_match_field" ); //$NON-NLS-1$
        newField.m_modifierUpdateOperation = rep.getStepAttributeString( id_step, i, "modifier_update_operation" ); //$NON-NLS-1$
        String policy = rep.getStepAttributeString( id_step, i, "modifier_policy" ); //$NON-NLS-1$
        if ( !Const.isEmpty( policy ) ) {
          newField.m_modifierOperationApplyPolicy = policy;
        }
        newField.m_JSON = rep.getStepAttributeBoolean( id_step, i, "json_field" ); //$NON-NLS-1$
        newField.insertNull = rep.getStepAttributeBoolean( id_step, i, "allow_null" ); //$NON-NLS-1$

        m_mongoFields.add( newField );
      }
    }

    nrfields = rep.countNrStepAttributes( id_step, "path_to_fields" ); //$NON-NLS-1$
    if ( nrfields > 0 ) {
      m_mongoIndexes = new ArrayList<MongoIndex>();

      for ( int i = 0; i < nrfields; i++ ) {
        MongoIndex newIndex = new MongoIndex();

        newIndex.m_pathToFields = rep.getStepAttributeString( id_step, i, "path_to_fields" ); //$NON-NLS-1$
        newIndex.m_drop = rep.getStepAttributeBoolean( id_step, i, "drop" ); //$NON-NLS-1$
        newIndex.m_unique = rep.getStepAttributeBoolean( id_step, i, "unique" ); //$NON-NLS-1$
        newIndex.m_sparse = rep.getStepAttributeBoolean( id_step, i, "sparse" ); //$NON-NLS-1$

        m_mongoIndexes.add( newIndex );
      }
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    if ( !Const.isEmpty( getHostnames() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_host", //$NON-NLS-1$
          getHostnames() );
    }
    if ( !Const.isEmpty( getPort() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_port", getPort() ); //$NON-NLS-1$
    }

    rep.saveStepAttribute( id_transformation, id_step, "use_all_replica_members", getUseAllReplicaSetMembers() ); //$NON-NLS-1$

    if ( !Const.isEmpty( getAuthenticationDatabaseName() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_auth_database", //$NON-NLS-1$
              getAuthenticationDatabaseName() );
    }
    if ( !Const.isEmpty( getAuthenticationUser() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_user", //$NON-NLS-1$
          getAuthenticationUser() );
    }
    if ( !Const.isEmpty( getAuthenticationPassword() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_password", //$NON-NLS-1$
          Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) );
    }

    rep.saveStepAttribute( id_transformation, id_step, "auth_mech", getAuthenticationMechanism() );

    rep.saveStepAttribute( id_transformation, id_step, "auth_kerberos", //$NON-NLS-1$
        getUseKerberosAuthentication() );

    if ( !Const.isEmpty( getDbName() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_db", getDbName() ); //$NON-NLS-1$
    }
    if ( !Const.isEmpty( getCollection() ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "mongo_collection", //$NON-NLS-1$
          getCollection() );
    }
    if ( !Const.isEmpty( m_batchInsertSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "batch_insert_size", //$NON-NLS-1$
          m_batchInsertSize );
    }

    rep.saveStepAttribute( id_transformation, id_step, "connect_timeout", //$NON-NLS-1$
        getConnectTimeout() );
    rep.saveStepAttribute( id_transformation, id_step, "socket_timeout", //$NON-NLS-1$
        getSocketTimeout() );
    rep.saveStepAttribute( id_transformation, id_step, "read_preference", //$NON-NLS-1$
        getReadPreference() );
    rep.saveStepAttribute( id_transformation, id_step, "write_concern", //$NON-NLS-1$
        getWriteConcern() );
    rep.saveStepAttribute( id_transformation, id_step, "w_timeout", getWTimeout() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "journaled_writes", //$NON-NLS-1$
        getJournal() );

    rep.saveStepAttribute( id_transformation, id_step, 0, "truncate", m_truncate ); //$NON-NLS-1$

    rep.saveStepAttribute( id_transformation, id_step, 0, "update", m_update ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "upsert", m_upsert ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "multi", m_multi ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, 0, "modifier_update", //$NON-NLS-1$
        m_modifierUpdate );
    rep.saveStepAttribute( id_transformation, id_step, 0, "write_retries", //$NON-NLS-1$
        m_writeRetries );
    rep.saveStepAttribute( id_transformation, id_step, 0, "write_retry_delay", //$NON-NLS-1$
        m_writeRetryDelay );

    if ( m_mongoFields != null && m_mongoFields.size() > 0 ) {
      for ( int i = 0; i < m_mongoFields.size(); i++ ) {
        MongoField field = m_mongoFields.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "incoming_field_name", field.m_incomingFieldName ); //$NON-NLS-1$
        rep.saveStepAttribute( id_transformation, id_step, i, "mongo_doc_path", //$NON-NLS-1$
            field.m_mongoDocPath );
        rep.saveStepAttribute( id_transformation, id_step, i, "use_incoming_field_name_as_mongo_field_name", //$NON-NLS-1$
            field.m_useIncomingFieldNameAsMongoFieldName );
        rep.saveStepAttribute( id_transformation, id_step, i, "update_match_field", field.m_updateMatchField ); //$NON-NLS-1$
        rep.saveStepAttribute( id_transformation, id_step, i,
            "modifier_update_operation", field.m_modifierUpdateOperation ); //$NON-NLS-1$
        rep.saveStepAttribute( id_transformation, id_step, i, "modifier_policy", //$NON-NLS-1$
            field.m_modifierOperationApplyPolicy );
        rep.saveStepAttribute( id_transformation, id_step, i, "json_field", //$NON-NLS-1$
            field.m_JSON );
        rep.saveStepAttribute( id_transformation, id_step, i, "allow_null", //$NON-NLS-1$
            field.insertNull );
      }
    }

    if ( m_mongoIndexes != null && m_mongoIndexes.size() > 0 ) {
      for ( int i = 0; i < m_mongoIndexes.size(); i++ ) {
        MongoIndex mongoIndex = m_mongoIndexes.get( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "path_to_fields", //$NON-NLS-1$
            mongoIndex.m_pathToFields );
        rep.saveStepAttribute( id_transformation, id_step, i, "drop", //$NON-NLS-1$
            mongoIndex.m_drop );
        rep.saveStepAttribute( id_transformation, id_step, i, "unique", //$NON-NLS-1$
            mongoIndex.m_unique );
        rep.saveStepAttribute( id_transformation, id_step, i, "sparse", //$NON-NLS-1$
            mongoIndex.m_sparse );
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#getDialogClassName()
   */
  @Override
  public String getDialogClassName() {
    return MongoDbOutputDialog.class.getCanonicalName(); //$NON-NLS-1$
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.pentaho.di.trans.step.BaseStepMeta#supportsErrorHandling()
   */
  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
