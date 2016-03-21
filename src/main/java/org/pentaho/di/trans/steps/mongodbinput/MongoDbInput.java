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

package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.AggregationOutput;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
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
import org.pentaho.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;

import java.util.List;

public class MongoDbInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
  // needed by
  // Translator2!!
  // $NON-NLS-1$

  private MongoDbInputMeta meta;
  private MongoDbInputData data;

  private boolean m_serverDetermined;
  private Object[] m_currentInputRowDrivingQuery = null;

  public MongoDbInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    try {
      if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery == null ) {
        m_currentInputRowDrivingQuery = getRow();

        if ( m_currentInputRowDrivingQuery == null ) {
          // no more input, no more queries to make
          setOutputDone();
          return false;
        }

        if ( !first ) {
          initQuery();
        }
      }

      if ( first ) {
        data.outputRowMeta = new RowMeta();
        meta.getFields( data.outputRowMeta, getStepname(), null, null, MongoDbInput.this );

        initQuery();
        first = false;

        data.init();
      }

      boolean
          hasNext =
          ( ( meta.getQueryIsPipeline() ? data.m_pipelineResult.hasNext() : data.cursor.hasNext() ) && !isStopped() );
      if ( hasNext ) {
        DBObject nextDoc = null;
        Object[] row = null;
        if ( meta.getQueryIsPipeline() ) {
          nextDoc = data.m_pipelineResult.next();
        } else {
          nextDoc = data.cursor.next();
        }

        if ( !meta.getQueryIsPipeline() && !m_serverDetermined ) {
          ServerAddress s = data.cursor.getServerAddress();
          if ( s != null ) {
            m_serverDetermined = true;
            logBasic(
                BaseMessages.getString( PKG, "MongoDbInput.Message.QueryPulledDataFrom", s.toString() ) ); //$NON-NLS-1$
          }
        }

        if ( meta.getOutputJson() || meta.getMongoFields() == null || meta.getMongoFields().size() == 0 ) {
          String json = nextDoc.toString();
          row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );
          int index = 0;

          row[index++] = json;
          putRow( data.outputRowMeta, row );
        } else {
          Object[][] outputRows = data.mongoDocumentToKettle( nextDoc, MongoDbInput.this );

          // there may be more than one row if the paths contain an array
          // unwind
          for ( int i = 0; i < outputRows.length; i++ ) {
            putRow( data.outputRowMeta, outputRows[i] );
          }
        }
      } else {
        if ( !meta.getExecuteForEachIncomingRow() ) {
          setOutputDone();

          return false;
        } else {
          m_currentInputRowDrivingQuery = null; // finished with this row
        }
      }

      return true;
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e ); //$NON-NLS-1$
      }
    }
  }

  protected void initQuery() throws KettleException, MongoDbException {

    // close any previous cursor
    if ( data.cursor != null ) {
      data.cursor.close();
    }

    // check logging level and only set to false if
    // logging level at least detailed
    if ( log.isDetailed() ) {
      m_serverDetermined = false;
    }

    String query = environmentSubstitute( meta.getJsonQuery() );
    String fields = environmentSubstitute( meta.getFieldsName() );
    if ( Const.isEmpty( query ) && Const.isEmpty( fields ) ) {
      if ( meta.getQueryIsPipeline() ) {
        throw new KettleException( BaseMessages
            .getString( MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline" ) ); //$NON-NLS-1$
      }

      data.cursor = data.collection.find();
    } else {

      if ( meta.getQueryIsPipeline() ) {
        if ( Const.isEmpty( query ) ) {
          throw new KettleException( BaseMessages
              .getString( MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline" ) ); //$NON-NLS-1$
        }

        if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
          // do field value substitution
          query = fieldSubstitute( query, getInputRowMeta(), m_currentInputRowDrivingQuery );
        }

        logDetailed( BaseMessages.getString( PKG, "MongoDbInput.Message.QueryPulledDataFrom", query ) );

        List<DBObject> pipeline = MongodbInputDiscoverFieldsImpl.jsonPipelineToDBObjectList( query );
        DBObject firstP = pipeline.get( 0 );
        DBObject[] remainder = null;
        if ( pipeline.size() > 1 ) {
          remainder = new DBObject[pipeline.size() - 1];
          for ( int i = 1; i < pipeline.size(); i++ ) {
            remainder[i - 1] = pipeline.get( i );
          }
        } else {
          remainder = new DBObject[0];
        }

        AggregationOutput result = data.collection.aggregate( firstP, remainder );
        data.m_pipelineResult = result.results().iterator();
      } else {
        if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
          // do field value substitution
          query = fieldSubstitute( query, getInputRowMeta(), m_currentInputRowDrivingQuery );

          fields = fieldSubstitute( fields, getInputRowMeta(), m_currentInputRowDrivingQuery );
        }

        logDetailed( BaseMessages.getString( PKG, "MongoDbInput.Message.ExecutingQuery", query ) );

        DBObject dbObject = (DBObject) JSON.parse( Const.isEmpty( query ) ? "{}" //$NON-NLS-1$
            : query );
        DBObject dbObject2 = (DBObject) JSON.parse( fields );
        data.cursor = data.collection.find( dbObject, dbObject2 );
      }
    }
  }

  @Override public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    if ( super.init( stepMetaInterface, stepDataInterface ) ) {
      meta = (MongoDbInputMeta) stepMetaInterface;
      data = (MongoDbInputData) stepDataInterface;

      String hostname = environmentSubstitute( meta.getHostnames() );
      int port = Const.toInt( environmentSubstitute( meta.getPort() ), MongoDbInputData.MONGO_DEFAULT_PORT );
      String db = environmentSubstitute( meta.getDbName() );
      String collection = environmentSubstitute( meta.getCollection() );

      try {
        if ( Const.isEmpty( db ) ) {
          throw new Exception( BaseMessages.getString( PKG, "MongoInput.ErrorMessage.NoDBSpecified" ) ); //$NON-NLS-1$
        }

        if ( Const.isEmpty( collection ) ) {
          throw new Exception(
              BaseMessages.getString( PKG, "MongoInput.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
        }

        if ( !Const.isEmpty( meta.getAuthenticationUser() ) ) {
          String
              authInfo =
              ( meta.getUseKerberosAuthentication() ? BaseMessages
                  .getString( PKG, "MongoDbInput.Message.KerberosAuthentication",
                      environmentSubstitute( meta.getAuthenticationUser() ) ) : BaseMessages
                      .getString( PKG, "MongoDbInput.Message.NormalAuthentication",
                        environmentSubstitute( meta.getAuthenticationUser() ) ) );

          logBasic( authInfo );
        }

        // init connection constructs a MongoCredentials object if necessary
        data.clientWrapper = MongoWrapperUtil.createMongoClientWrapper( meta, this, log );
        data.collection = data.clientWrapper.getCollection( db, collection );

        if ( !( (MongoDbInputMeta) stepMetaInterface ).getOutputJson() ) {
          ( (MongoDbInputData) stepDataInterface )
              .setMongoFields( ( (MongoDbInputMeta) stepMetaInterface ).getMongoFields() );
        }

        return true;
      } catch ( Exception e ) {
        logError( BaseMessages
            .getString( PKG, "MongoDbInput.ErrorConnectingToMongoDb.Exception", hostname, "" //$NON-NLS-1$ //$NON-NLS-2$
                    + port, db, collection ), e );
        return false;
      }
    } else {
      return false;
    }
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( data.cursor != null ) {
      try {
        data.cursor.close();
      } catch ( MongoDbException e ) {
        log.logError( e.getMessage() );
      }
    }
    if ( data.clientWrapper != null ) {
      try {
        data.clientWrapper.dispose();
      } catch ( MongoDbException e ) {
        log.logError( e.getMessage() );
      }
    }

    super.dispose( smi, sdi );
  }
}
