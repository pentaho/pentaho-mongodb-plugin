/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2025 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 ******************************************************************************/
package org.pentaho.di.trans.steps.mongodbinput;

import org.apache.commons.collections.CollectionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepHelper;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.mongodb.MongoDBHelper;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.pentaho.reporting.libraries.base.util.StringUtils;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MongoDbInputHelper extends BaseStepHelper {

  private final MongoDbInputMeta mongoDbInputMeta;
  private static final String UNABLE_TO_CONNECT_LABEL = "MongoDbInputDialog.ErrorMessage.UnableToConnect";
  private static final Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
  private static final String MISSING_CONN_DETAILS = "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails";
  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String TEST_CONNECTION = "testConnection";
  private static final String GET_DB_NAMES = "getDBNames";
  private static final String GET_COLLECTION_NAMES = "getCollectionNames";
  private static final String GET_PREFERENCES = "getPreferences";
  private static final String GET_TAGSET = "getTagSet";
  private static final String TEST_TAGSET = "testTagSet";
  private static final String GET_FIELDS = "getFields";

  public MongoDbInputHelper( MongoDbInputMeta mongoDbInputMeta ) {
    this.mongoDbInputMeta = mongoDbInputMeta;
  }

  @Override
    protected JSONObject handleStepAction( String method, TransMeta transMeta, Map<String, String> queryParams ) {
    JSONObject response = new JSONObject();
    MongoDBHelper mongoDBHelper = new MongoDBHelper();
    try {
      switch ( method ) {
        case TEST_CONNECTION:
          response = mongoDBHelper.testConnectionAction( transMeta, mongoDbInputMeta );
          break;
        case GET_DB_NAMES:
          response = mongoDBHelper.getDBNamesAction( transMeta, mongoDbInputMeta );
          break;
        case GET_COLLECTION_NAMES:
          response = mongoDBHelper.getCollectionNamesAction( transMeta, mongoDbInputMeta );
          break;
        case GET_PREFERENCES:
          response = mongoDBHelper.getPreferencesAction();
          break;
        case GET_TAGSET:
          response = getTagSetAction( transMeta );
          break;
        case TEST_TAGSET:
          response = testTagSetAction( transMeta );
          break;
        case GET_FIELDS:
          response = getFieldsAction( transMeta, queryParams );
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

  private JSONObject getTagSetAction( TransMeta transMeta ) {
    JSONObject response = new JSONObject();
    List<String> tags;
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

  private JSONObject testTagSetAction( TransMeta transMeta ) throws MongoDbException {
    JSONObject response = new JSONObject();
    List<DBObject> tagSets = new ArrayList<>();

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
        MongoDBHelper.setupDBObjects( tagSets, tagSet );
      }

      if ( CollectionUtils.isEmpty( tagSets ) ) {
        return MongoDBHelper.errorResponse( response,
                        BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
                        BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets" ) );
      }

      wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbInputMeta, transMeta, transMeta.getLogChannel() );
      List<String> replicaSetTags = MongoDBHelper.getReplicaMemberBasedOnTagSet( tagSets, wrapper );
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

  private JSONObject getFieldsAction( TransMeta transMeta, Map<String, String> queryParams ) {
    JSONObject response = new JSONObject();

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

  private boolean checkForUnresolved( MongoDbInputMeta meta, TransMeta transMeta, JSONObject response ) {
    String jsonQuery = meta.getJsonQuery() != null ? meta.getJsonQuery() : "";
    String query = transMeta.environmentSubstitute( jsonQuery );

        // Defensive null check
    if ( query == null ) {
      query = "";
    }

    boolean notOk = ( query.contains( "${" ) || query.contains( "?{" ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( notOk ) {
      MongoDBHelper.errorResponse( response,
                    BaseMessages.getString( PKG, UNABLE_TO_CONNECT_LABEL ),
                    BaseMessages.getString( PKG,
                            "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs" ) );
    }

    return !notOk;
  }

  JSONArray setFieldResponse( List<MongoField> mongoFields ) {
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
}
