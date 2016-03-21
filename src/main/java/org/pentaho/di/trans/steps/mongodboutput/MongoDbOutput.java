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

import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.WriteResult;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class providing an output step for writing data to a MongoDB collection. Supports insert, truncate, upsert,
 * multi-update (update all matching docs) and modifier update (update only certain fields) operations. Can also create
 * and drop indexes based on one or more fields.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MongoDbOutput extends BaseStep implements StepInterface {
  private static Class<?> PKG = MongoDbOutputMeta.class;

  protected MongoDbOutputMeta m_meta;
  protected MongoDbOutputData m_data;

  protected MongoDbOutputData.MongoTopLevel m_mongoTopLevelStructure = MongoDbOutputData.MongoTopLevel.INCONSISTENT;

  /**
   * The batch size to use for insert operation
   */
  protected int m_batchInsertSize = 100;

  /**
   * Holds a batch of rows converted to documents
   */
  protected List<DBObject> m_batch;

  /**
   * Holds an original batch of rows (corresponding to the converted documents)
   */
  protected List<Object[]> m_batchRows;

  protected int m_writeRetries = MongoDbOutputMeta.RETRIES;
  protected int m_writeRetryDelay = MongoDbOutputMeta.RETRY_DELAY;

  public MongoDbOutput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] row = getRow();

    if ( row == null ) {
      // no more output

      // check any remaining buffered objects
      if ( m_batch != null && m_batch.size() > 0 ) {
        try {
          doBatch();
        } catch ( MongoDbException e ) {
          throw new KettleException( e );
        }
      }

      // INDEXING - http://www.mongodb.org/display/DOCS/Indexes
      // Indexing is computationally expensive - it needs to be
      // done after all data is inserted and done in the BACKGROUND.

      // UNIQUE indexes (prevent duplicates on the
      // keys in the index) and SPARSE indexes (don't index docs that
      // don't have the key field) - current limitation is that SPARSE
      // indexes can only have a single field

      List<MongoDbOutputMeta.MongoIndex> indexes = m_meta.getMongoIndexes();
      if ( indexes != null && indexes.size() > 0 ) {
        logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.ApplyingIndexOpps" ) ); //$NON-NLS-1$
        try {
          m_data.applyIndexes( indexes, log, m_meta.getTruncate() );
        } catch ( MongoDbException e ) {
          throw new KettleException( e );
        }
      }

      disconnect();
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      m_batchInsertSize = 100;

      String batchInsert = environmentSubstitute( m_meta.getBatchInsertSize() );
      if ( !Const.isEmpty( batchInsert ) ) {
        m_batchInsertSize = Integer.parseInt( batchInsert );
      }
      m_batch = new ArrayList<DBObject>( m_batchInsertSize );
      m_batchRows = new ArrayList<Object[]>();

      // output the same as the input
      m_data.setOutputRowMeta( getInputRowMeta() );

      // scan for top-level JSON document insert and validate
      // field specification in this case.
      m_data.m_hasTopLevelJSONDocInsert = MongoDbOutputData.scanForInsertTopLevelJSONDoc( m_meta.getMongoFields() );

      // first check our incoming fields against our meta data for
      // fields to
      // insert
      // this fields is came to step input
      RowMetaInterface rmi = getInputRowMeta();
      // this fields we are going to use for mongo output
      List<MongoDbOutputMeta.MongoField> mongoFields = m_meta.getMongoFields();
      checkInputFieldsMatch( rmi, mongoFields );

      // copy and initialize mongo fields
      m_data.setMongoFields( m_meta.getMongoFields() );
      m_data.init( MongoDbOutput.this );

      // check truncate
      if ( m_meta.getTruncate() ) {
        try {
          logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.TruncatingCollection" ) ); //$NON-NLS-1$
          m_data.getCollection().remove();
        } catch ( Exception m ) {
          disconnect();
          throw new KettleException( m.getMessage(), m );
        }
      }
    }

    if ( !isStopped() ) {

      if ( m_meta.getUpdate() ) {
        DBObject
            updateQuery =
            MongoDbOutputData.getQueryObject( m_data.getMongoFields(), getInputRowMeta(), row, MongoDbOutput.this,
                m_mongoTopLevelStructure );

        if ( log.isDebug() ) {
          logDebug(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.Debug.QueryForUpsert", updateQuery ) ); //$NON-NLS-1$
        }

        if ( updateQuery != null ) {
          // i.e. we have some non-null incoming query field values
          DBObject insertUpdate = null;

          // get the record to update the match with
          if ( !m_meta.getModifierUpdate() ) {
            // complete record replace or insert

            insertUpdate =
                MongoDbOutputData.kettleRowToMongo( m_data.getMongoFields(), getInputRowMeta(), row, MongoDbOutput.this,
                    m_mongoTopLevelStructure, m_data.m_hasTopLevelJSONDocInsert );
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Debug.InsertUpsertObject", //$NON-NLS-1$
                  insertUpdate ) );
            }

          } else {

            // specific field update (or insert)
            try {
              insertUpdate =
                  m_data.getModifierUpdateObject( m_data.getMongoFields(), getInputRowMeta(), row, MongoDbOutput.this,
                      m_mongoTopLevelStructure );
            } catch ( MongoDbException e ) {
              throw new KettleException( e );
            }
            if ( log.isDebug() ) {
              logDebug( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Debug.ModifierUpdateObject", //$NON-NLS-1$
                  insertUpdate ) );
            }
          }

          if ( insertUpdate != null ) {
            commitUpdate( updateQuery, insertUpdate, row );
          }
        }
      } else {
        // straight insert

        DBObject
            mongoInsert =
            MongoDbOutputData.kettleRowToMongo( m_data.getMongoFields(), getInputRowMeta(), row, MongoDbOutput.this,
                m_mongoTopLevelStructure, m_data.m_hasTopLevelJSONDocInsert );

        if ( mongoInsert != null ) {
          m_batch.add( mongoInsert );
          m_batchRows.add( row );
        }
        if ( m_batch.size() == m_batchInsertSize ) {
          logDetailed( BaseMessages.getString( PKG, "MongoDbOutput.Messages.CommitingABatch" ) ); //$NON-NLS-1$
          try {
            doBatch();
          } catch ( MongoDbException e ) {
            throw new KettleException( e );
          }
        }
      }
    }

    return true;
  }

  protected void commitUpdate( DBObject updateQuery, DBObject insertUpdate, Object[] row ) throws KettleException {

    int retrys = 0;
    MongoException lastEx = null;

    while ( retrys <= m_writeRetries && !isStopped() ) {
      WriteResult result = null;
      try {
        // TODO It seems that doing an update() via a secondary node does not
        // generate any sort of exception or error result! (at least via
        // driver version 2.11.1). Transformation completes successfully
        // but no updates are made to the collection.
        // This is unlike doing an insert(), which generates
        // a MongoException if you are not talking to the primary. So we need
        // some logic to check whether or not the connection configuration
        // contains the primary in the replica set and give feedback if it
        // doesnt
        try {
          result = m_data.getCollection().update( updateQuery, insertUpdate, m_meta.getUpsert(), m_meta.getMulti() );
        } catch ( MongoDbException e ) {
          throw new MongoException( e.getMessage(), e );
        }
      } catch ( MongoException me ) {
        lastEx = me;
        retrys++;
        if ( retrys <= m_writeRetries ) {
          logError( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.ErrorWritingToMongo", //$NON-NLS-1$
              me.toString() ) );
          logBasic(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.Message.Retry", m_writeRetryDelay ) ); //$NON-NLS-1$
          try {
            Thread.sleep( m_writeRetryDelay * 1000 );
            // CHECKSTYLE:OFF
          } catch ( InterruptedException e ) {
            // CHECKSTYLE:ON
          }
        }
      }

      if ( result != null ) {
        break;
      }
    }

    if ( ( retrys > m_writeRetries || isStopped() ) && lastEx != null ) {

      // Send this one to the error stream if doing error handling
      if ( getStepMeta().isDoingErrorHandling() ) {
        putError( getInputRowMeta(), row, 1, lastEx.getMessage(), "", "MongoDbOutput" );
      } else {
        throw new KettleException( lastEx );
      }
    }
  }

  protected WriteResult batchRetryUsingSave( boolean lastRetry )
    throws MongoException, KettleException, MongoDbException {
    WriteResult result = null;
    int count = 0;
    logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.CurrentBatchSize", m_batch.size() ) );
    for ( int i = 0, len = m_batch.size(); i < len; i++ ) {
      DBObject toTry = m_batch.get( i );
      Object[] correspondingRow = m_batchRows.get( i );
      try {
        result = m_data.getCollection().save( toTry );
        count++;
      } catch ( MongoException ex ) {
        if ( !lastRetry ) {
          logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.SuccessfullySavedXDocuments", count ) );
          m_batch = copyExceptFirst( count, m_batch );
          m_batchRows = copyExceptFirst( count, m_batchRows );
          throw ex;
        }

        // Send this one to the error stream if doing error handling
        if ( getStepMeta().isDoingErrorHandling() ) {
          putError( getInputRowMeta(), correspondingRow, 1, ex.getMessage(), "", "MongoDbOutput" );
        } else {
          m_batch = copyExceptFirst( i + 1, m_batch );
          m_batchRows = copyExceptFirst( i + 1, m_batchRows );
          throw ex;
        }
      }
    }

    m_batch.clear();
    m_batchRows.clear();

    logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.SuccessfullySavedXDocuments", count ) );

    return result;
  }

  private static <T> List<T> copyExceptFirst( int amount, List<T> list ) {
    return new ArrayList<T>( list.subList( amount, list.size() ) );
  }

  protected void doBatch() throws KettleException, MongoDbException {
    int retries = 0;
    MongoException lastEx = null;

    while ( retries <= m_writeRetries && !isStopped() ) {
      WriteResult result = null;
      try {
        if ( retries == 0 ) {
          result = m_data.getCollection().insert( m_batch );
        } else {
          // fall back to save
          logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.SavingIndividualDocsInCurrentBatch" ) );
          result = batchRetryUsingSave( retries == m_writeRetries );
        }
      } catch ( MongoException me ) {
        // avoid exception if a timeout issue occurred and it was exactly the first attempt
        boolean shouldNotBeAvoided = !isTimeoutException( me ) && ( retries == 0 );
        if ( shouldNotBeAvoided ) {
          lastEx = me;
        }
        retries++;
        if ( retries <= m_writeRetries ) {
          if ( shouldNotBeAvoided ) {
            // skip logging error
            // however do not skip saving elements separately during next attempt to prevent losing data
            logError( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.ErrorWritingToMongo", //$NON-NLS-1$
                me.toString() ) );
            logBasic( BaseMessages
                .getString( PKG, "MongoDbOutput.Messages.Message.Retry", m_writeRetryDelay ) ); //$NON-NLS-1$
          }
          try {
            Thread.sleep( m_writeRetryDelay * 1000 );
            // CHECKSTYLE:OFF
          } catch ( InterruptedException e ) {
            // CHECKSTYLE:ON
          }
        }
        // throw new KettleException(me.getMessage(), me);
      }

      if ( result != null ) {
        break;
      }
    }

    if ( ( retries > m_writeRetries || isStopped() ) && lastEx != null ) {
      throw new KettleException( lastEx );
    }

    m_batch.clear();
    m_batchRows.clear();
  }

  private static boolean isTimeoutException( MongoException me ) {
    return ( me instanceof MongoExecutionTimeoutException );
  }

  @Override public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    if ( super.init( stepMetaInterface, stepDataInterface ) ) {
      m_meta = (MongoDbOutputMeta) stepMetaInterface;
      m_data = (MongoDbOutputData) stepDataInterface;

      if ( !Const.isEmpty( m_meta.getWriteRetries() ) ) {
        m_writeRetries = Const.toInt( m_meta.getWriteRetries(), MongoDbOutputMeta.RETRIES );
      }

      if ( !Const.isEmpty( m_meta.getWriteRetryDelay() ) ) {
        m_writeRetryDelay = Const.toInt( m_meta.getWriteRetryDelay(), MongoDbOutputMeta.RETRY_DELAY );
      }

      String hostname = environmentSubstitute( m_meta.getHostnames() );
      int port = Const.toInt( environmentSubstitute( m_meta.getPort() ), 27017 );
      String db = environmentSubstitute( m_meta.getDbName() );
      String collection = environmentSubstitute( m_meta.getCollection() );

      try {
        if ( Const.isEmpty( db ) ) {
          throw new Exception(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NoDBSpecified" ) ); //$NON-NLS-1$
        }

        if ( Const.isEmpty( collection ) ) {
          throw new Exception(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NoCollectionSpecified" ) ); //$NON-NLS-1$
        }

        if ( !Const.isEmpty( m_meta.getAuthenticationUser() ) ) {
          String
              authInfo =
              ( m_meta.getUseKerberosAuthentication() ? BaseMessages
                  .getString( PKG, "MongoDbOutput.Message.KerberosAuthentication",
                      environmentSubstitute( m_meta.getAuthenticationUser() ) ) : BaseMessages
                      .getString( PKG, "MongoDbOutput.Message.NormalAuthentication",
                          environmentSubstitute( m_meta.getAuthenticationUser() ) ) );

          logBasic( authInfo );
        }
        m_data.setConnection( MongoWrapperUtil
            .createMongoClientWrapper( m_meta, this, log ) ); //MongoDbOutputData.connect( m_meta, this, log ) );

        if ( Const.isEmpty( collection ) ) {
          throw new KettleException(
              BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NoCollectionSpecified" ) ); //$NON-NLS-1$
        }
        m_data.createCollection( db, collection );
        m_data.setCollection( m_data.getConnection().getCollection( db, collection ) );

        try {
          m_mongoTopLevelStructure =
              MongoDbOutputData.checkTopLevelConsistency( m_meta.getMongoFields(), MongoDbOutput.this );

          if ( m_mongoTopLevelStructure == MongoDbOutputData.MongoTopLevel.INCONSISTENT ) {
            logError(
                BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.InconsistentMongoTopLevel" ) ); //$NON-NLS-1$
            return false;
          }
        } catch ( KettleException e ) {
          logError( e.getMessage() );
          return false;
        }

        return true;
      } catch ( UnknownHostException ex ) {
        logError( BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.UnknownHost", hostname ),
            ex ); //$NON-NLS-1$
        return false;
      } catch ( Exception e ) {
        logError( BaseMessages
            .getString( PKG, "MongoDbOutput.Messages.Error.ProblemConnecting", hostname, "" //$NON-NLS-1$ //$NON-NLS-2$
                + port ), e );
        return false;
      }
    }

    return false;
  }

  protected void disconnect() {
    if ( m_data != null ) {
      try {
        m_data.getConnection().dispose();
      } catch ( MongoDbException e ) {
        log.logError( e.getMessage() );
      }
    }
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    disconnect();
    super.dispose( smi, sdi );
  }

  final void checkInputFieldsMatch( RowMetaInterface rmi, List<MongoDbOutputMeta.MongoField> mongoFields )
    throws KettleException {
    Set<String> expected = new HashSet<String>( mongoFields.size(), 1 );
    Set<String> actual = new HashSet<String>( rmi.getFieldNames().length, 1 );
    for ( MongoDbOutputMeta.MongoField field : mongoFields ) {
      String mongoMatch = environmentSubstitute( field.m_incomingFieldName );
      expected.add( mongoMatch );
    }
    for ( int i = 0; i < rmi.size(); i++ ) {
      String metaFieldName = rmi.getValueMeta( i ).getName();
      actual.add( metaFieldName );
    }

    // check that all expected fields is available in step input meta
    if ( !actual.containsAll( expected ) ) {
      // in this case some fields willn't be found in input step meta
      expected.removeAll( actual );
      StringBuffer b = new StringBuffer();
      for ( String name : expected ) {
        b.append( "'" ).append( name ).append( "', " );
      }
      throw new KettleException( BaseMessages
          .getString( PKG, "MongoDbOutput.Messages.MongoField.Error.FieldsNotFoundInMetadata", b.toString() ) );
    }

    boolean found = actual.removeAll( expected );
    if ( !found ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "MongoDbOutput.Messages.Error.NotInsertingAnyFields" ) ); //$NON-NLS-1$
    }

    if ( !actual.isEmpty() ) {
      // we have some fields that will not be inserted.
      StringBuffer b = new StringBuffer();
      for ( String name : actual ) {
        b.append( "'" ).append( name ).append( "', " );
      }
      // just put a log record on it
      logBasic( BaseMessages.getString( PKG, "MongoDbOutput.Messages.FieldsNotToBeInserted", b.toString() ) );
    }
  }
}
