/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.mongodb.MongoDBHelper;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.pentaho.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;
import org.pentaho.reporting.libraries.base.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MongoDbInput extends BaseStep implements StepInterface {
  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String MISSING_CONN_DETAILS = "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails";
  private static final String UNABLE_TO_CONNECT_LABEL = "MongoDbInputDialog.ErrorMessage.UnableToConnect";
  private static final Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
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
        } else if ( data.outputRowMeta == null ) {
          data.outputRowMeta = getInputRowMeta().clone();
        }
      }

      if ( first ) {
        if ( data.outputRowMeta == null ) {
          data.outputRowMeta = new RowMeta();
        }

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

        if ( meta.getOutputJson() || meta.getMongoFields() == null || meta.getMongoFields().isEmpty() ) {
          String json = JSON.serialize( nextDoc );
          row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

          if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
            // Add the incoming columns at start and at the end of the incoming columns add json output.
            RowMetaInterface inputRowMeta = getInputRowMeta();
            appendTheIncomingRowsAtStart( row, inputRowMeta );
            row[ inputRowMeta.size() ] = json;
          } else {
            // If there are no incoming columns then adding only json output to output row
            row[ 0 ] = json;
          }
          putRow( data.outputRowMeta, row );
        } else {
          Object[][] outputRows = data.mongoDocumentToKettle( nextDoc, MongoDbInput.this );

          // there may be more than one row if the paths contain an array
          // unwind
          for ( Object[] outputRow : outputRows ) {
            // Add all the incoming column values if they are not null
            if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
              appendTheIncomingRowsAtStart( outputRow, getInputRowMeta() );
            }

            putRow( data.outputRowMeta, outputRow );
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

  private void appendTheIncomingRowsAtStart( Object[] row, RowMetaInterface inputRowMeta ) {
    for ( int columnIndex = 0; columnIndex < inputRowMeta.size(); columnIndex++ ) {
      if ( m_currentInputRowDrivingQuery[ columnIndex ] != null ) {
        row[ columnIndex ] = m_currentInputRowDrivingQuery[ columnIndex ];
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

        // Utilize MongoDB cursor class
        data.m_pipelineResult = data.collection.aggregate( firstP, remainder, meta.isAllowDiskUse() );

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

  @Override
  public JSONObject doAction( String fieldName, StepMetaInterface stepMetaInterface, TransMeta transMeta,
                              Trans trans, Map<String, String> queryParamToValues ) {
    JSONObject response = new JSONObject();
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    try {
      this.setStepMetaInterface( stepMetaInterface );

      MongoDBHelper mongoDBHelper = new MongoDBHelper();
      MongoDbInputMeta mongoDbInputMeta = (MongoDbInputMeta) getStepMetaInterface();

      switch ( fieldName ) {
        case "testConnection":
          response = mongoDBHelper.testConnectionAction( transMeta, mongoDbInputMeta );
          break;
        case "getDBNames":
          response = mongoDBHelper.getDBNamesAction( transMeta, mongoDbInputMeta );
          break;
        case "getCollectionNames":
          response = mongoDBHelper.getCollectionNamesAction( transMeta, mongoDbInputMeta );
          break;
        case "getPreferences":
          response = mongoDBHelper.getPreferencesAction();
          break;
        case "getTagSet":
          response = getTagSetAction();
          break;
        case "testTagSet":
          response = testTagSetAction();
          break;
        case "getFields":
          response = getFieldsAction( queryParamToValues );
          break;
        default:
          response.put( StepInterface.ACTION_STATUS, StepInterface.FAILURE_METHOD_NOT_RESPONSE );
          break;
      }
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
      response.put( StepInterface.ACTION_STATUS, StepInterface.FAILURE_METHOD_NOT_RESPONSE );
    }

    return response;
  }

  private JSONObject getTagSetAction() {
    JSONObject response = new JSONObject();
    List<String> tags;

    MongoDbInputMeta mongoDbInputMeta = (MongoDbInputMeta) getStepMetaInterface();
    TransMeta transMeta = getTransMeta();
    if ( StringUtils.isEmpty( mongoDbInputMeta.getHostnames() ) ) {
      return MongoDBHelper.errorResponse(
          response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS, "host name(s)" ) );
    }

    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbInputMeta, transMeta, transMeta.getLogChannel() );
      try {
        tags = wrapper.getAllTags();
      } finally {
        wrapper.dispose();
      }
    } catch ( Exception exception ) {
      return MongoDBHelper.errorResponse( response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          exception );
    }

    if ( CollectionUtils.isEmpty( tags ) ) {
      return MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ), BaseMessages.getString( PKG, "MongoDbInputDialog.Info.Message.NoTagSetsDefinedOnServer" ) );
    } else {
      JSONArray jsonArray = new JSONArray();
      for ( String tag : tags ) {
        JSONObject jsonObject = new JSONObject();
        if ( !tag.startsWith( "{" ) ) {
          tag = "{" + tag;
        }
        if ( !tag.endsWith( "}" ) ) {
          tag += "}";
        }

        jsonObject.put( "tag_set", tag );
        jsonArray.add( jsonObject );
      }

      response.put( "tag_sets", jsonArray );
      response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
      return response;
    }
  }

  private JSONObject testTagSetAction() throws MongoDbException {
    JSONObject response = new JSONObject();
    List<DBObject> tagSets = new ArrayList<>();

    MongoDbInputMeta mongoDbInputMeta = (MongoDbInputMeta) getStepMetaInterface();
    TransMeta transMeta = getTransMeta();
    if ( StringUtils.isEmpty( mongoDbInputMeta.getHostnames() ) ) {
      return MongoDBHelper.errorResponse( response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoConnectionDetailsSupplied" ) );
    }

    if ( CollectionUtils.isEmpty( mongoDbInputMeta.getReadPrefTagSets() ) ) {
      return MongoDBHelper.errorResponse( response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoTagSetsDefined" ) );
    }

    MongoClientWrapper wrapper = null;
    try {
      for ( String tagSet : mongoDbInputMeta.getReadPrefTagSets() ) {
        setupDBObjects( tagSets, tagSet );
      }

      if ( CollectionUtils.isEmpty( tagSets ) ) {
        return MongoDBHelper.errorResponse( response,
            BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
            BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets" ) );
      }

      wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbInputMeta, transMeta, transMeta.getLogChannel() );
      List<String> replicaSetTags = getReplicaMemberBasedOnTagSet( tagSets, wrapper );
      if ( CollectionUtils.isEmpty( replicaSetTags ) ) {
        return MongoDBHelper.errorResponse( response,
            BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
            BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage..NoReplicaSetMembersMatchTagSets" ) );
      }

      StringBuilder builder = new StringBuilder();
      builder.append( "\n" );
      for ( String replicaSetTag : replicaSetTags ) {
        builder.append( replicaSetTag ).append( "\n" );
      }

      response.put( "replicaSetTag", builder.toString() );
    } catch ( Exception ex ) {
      MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ), ex );
      return response;
    } finally {
      if ( wrapper != null ) {
        wrapper.dispose();
      }
    }

    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  private JSONObject getFieldsAction( Map<String, String> queryParams ) {
    JSONObject response = new JSONObject();

    MongoDbInputMeta mongoDbInputMeta = (MongoDbInputMeta) getStepMetaInterface();
    TransMeta transMeta = getTransMeta();

    String missingConDetails = MongoDBHelper.validateRequestForFields( mongoDbInputMeta );
    if ( !StringUtils.isEmpty( missingConDetails ) ) {
      return MongoDBHelper.errorResponse( response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS, missingConDetails ) );
    }

    boolean current = mongoDbInputMeta.getExecuteForEachIncomingRow();
    mongoDbInputMeta.setExecuteForEachIncomingRow( false );
    if ( !checkForUnresolved( mongoDbInputMeta, transMeta, response ) ) {
      return response;
    }

    try {
      int numDocsToSample = Integer.parseInt( queryParams.get( "sampleSize" ) );
      MongoProperties.Builder propertiesBuilder = MongoWrapperUtil.createPropertiesBuilder( mongoDbInputMeta, transMeta );
      MongoDbInputData.getMongoDbInputDiscoverFieldsHolder().getMongoDbInputDiscoverFields().discoverFields( propertiesBuilder, mongoDbInputMeta.getDbName(),
          mongoDbInputMeta.getCollection(), mongoDbInputMeta.getJsonQuery(), mongoDbInputMeta.getFieldsName(), mongoDbInputMeta.getQueryIsPipeline(), numDocsToSample,
          mongoDbInputMeta, transMeta, new DiscoverFieldsCallback() {
            @Override
            public void notifyFields( final List<MongoField> fields ) {
              if ( !fields.isEmpty() ) {
                response.put( "fields", setFieldResponse( fields ) );
                response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
              } else {
                response.put( ERROR_MESSAGE, BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoFieldsFound" ) );
                response.put( StepInterface.ACTION_STATUS, StepInterface.FAILURE_RESPONSE );
              }
            }
            @Override
            public void notifyException( Exception exception ) {
              MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ), exception );
            }
          } );
    } catch ( KettleException e ) {
      return MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ), e );
    } finally {
      mongoDbInputMeta.setExecuteForEachIncomingRow( current );
    }

    return response;
  }

  public List<String> getReplicaMemberBasedOnTagSet( List<DBObject> tagSets, MongoClientWrapper wrapper ) throws KettleException {
    List<String> satisfy;
    try {
      try {
        satisfy = wrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
      } catch ( MongoDbException e ) {
        throw new KettleException( e );
      }
    } finally {
      try {
        wrapper.dispose();
      } catch ( MongoDbException e ) {
        //Ignore
      }
    }
    return satisfy;
  }

  public void setupDBObjects( List<DBObject> tagSets, String tagSet ) {
    String set = tagSet;
    if ( !tagSet.startsWith( "{" ) ) {
      set = "{" + tagSet;
    }

    if ( !tagSet.endsWith( "}" ) ) {
      set = set + "}";
    }

    DBObject setO = BasicDBObject.parse( set );
    tagSets.add(setO);
  }

  private JSONArray setFieldResponse( List<MongoField> mongoFields ) {
    JSONArray jsonArray = new JSONArray();
    for ( MongoField mongoField : mongoFields ) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put( "field_name", mongoField.m_fieldName );
      jsonObject.put( "field_path", mongoField.m_fieldPath );
      jsonObject.put( "field_type", mongoField.m_kettleType );
      jsonObject.put( "indexed_vals", mongoField.m_indexedVals );
      jsonObject.put( "field_array_index", mongoField.m_arrayIndexInfo );
      jsonObject.put( "occurrence_fraction", mongoField.m_occurenceFraction );
      jsonObject.put( "field_disparate_types", mongoField.m_disparateTypes );
      jsonArray.add( jsonObject );
    }

    return jsonArray;
  }

  private boolean checkForUnresolved( MongoDbInputMeta meta, TransMeta transMeta, JSONObject response ) {
    String jsonQuery = meta.getJsonQuery() != null ? meta.getJsonQuery() : "";
    String query = transMeta.environmentSubstitute( jsonQuery );

    boolean notOk = ( query.contains( "${" ) || query.contains( "?{" ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( notOk ) {
      MongoDBHelper.errorResponse( response,
          BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
          BaseMessages.getString( PKG,
          "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs" ) );
    }

    return !notOk;
  }
}
