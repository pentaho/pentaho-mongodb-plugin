/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2025 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/
package org.pentaho.di.trans.steps.mongodboutput;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepHelper;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.mongodb.MongoDBHelper;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.reporting.libraries.base.util.StringUtils;
import java.util.List;
import java.util.Map;

public class MongoDbOutputHelper extends BaseStepHelper {

  private static final Class<?> PKG = MongoDbOutputHelper.class;
  private final MongoDbOutputMeta mongoDbOutputMeta;
  private static final String TEST_CONNECTION = "testConnection";
  private static final String GET_DB_NAMES = "getDBNames";
  private static final String GET_COLLECTION_NAMES = "getCollectionNames";
  private static final String GET_PREFERENCES = "getPreferences";
  private static final String WRITE_CONCERNS = "writeConcerns";
  private static final String GET_DOCUMENT_FIELDS = "getDocumentFields";
  private static final String PREVIEW_DOCUMENT_STRUCTURE = "previewDocumentStructure";
  private static final String SHOW_INDEXES = "showIndexes";
  private static final String MISSING_CONN_DETAILS = "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails";
  private static final String MISSING_CONN_DETAILS_TITLE = "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.title";

  public MongoDbOutputHelper( MongoDbOutputMeta mongoDbOutputMeta ) {
    this.mongoDbOutputMeta = mongoDbOutputMeta;
  }

  @Override
    protected JSONObject handleStepAction( String method, TransMeta transMeta, Map<String, String> queryParams ) {
    JSONObject response = new JSONObject();
    MongoDBHelper mongoDBHelper = new MongoDBHelper();
    try {
      switch ( method ) {
        case TEST_CONNECTION:
          response = mongoDBHelper.testConnectionAction( transMeta, mongoDbOutputMeta );
          break;
        case GET_DB_NAMES:
          response = mongoDBHelper.getDBNamesAction( transMeta, mongoDbOutputMeta );
          break;
        case GET_COLLECTION_NAMES:
          response = mongoDBHelper.getCollectionNamesAction( transMeta, mongoDbOutputMeta );
          break;
        case GET_PREFERENCES:
          response = mongoDBHelper.getPreferencesAction();
          break;
        case WRITE_CONCERNS:
          response = getWriteConcernsAction( transMeta );
          break;
        case GET_DOCUMENT_FIELDS:
          response = getDocumentFieldsAction( transMeta );
          break;
        case PREVIEW_DOCUMENT_STRUCTURE:
          response = previewDocumentStructureAction( transMeta );
          break;
        case SHOW_INDEXES:
          response = showIndexesAction( transMeta );
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

  private JSONObject getWriteConcernsAction( TransMeta transMeta ) {
    JSONObject response = new JSONObject();
    String hostname = transMeta.environmentSubstitute( mongoDbOutputMeta.getHostnames() );

    if ( !Utils.isEmpty( hostname ) ) {
      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbOutputMeta, transMeta, transMeta.getLogChannel() );
        List<String> writeConcerns;
        try {
          writeConcerns = wrapper.getLastErrorModes();
          response.put( WRITE_CONCERNS, writeConcerns );
        } finally {
          wrapper.dispose();
        }
      } catch ( Exception exception ) {
        MongoDBHelper.errorResponse( response,
                        BaseMessages.getString( PKG, "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ),
                        exception );
        return response;
      }
    } else {
      MongoDBHelper.errorResponse( response,
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS, "host name(s)" ) );
      return response;
    }
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  private JSONObject getDocumentFieldsAction( TransMeta transMeta ) {
    JSONObject response = new JSONObject();

    String missingConDetails = MongoDBHelper.validateRequestForFields( mongoDbOutputMeta );
    if ( !StringUtils.isEmpty( missingConDetails ) ) {
      return MongoDBHelper.errorResponse( response,
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS, missingConDetails ) );
    }

    try {
      RowMetaInterface rowMetaInterface = transMeta.getPrevStepFields( mongoDbOutputMeta.getParentStepMeta().getName() );
      response.put( "fields", setFieldResponse( rowMetaInterface.getValueMetaList() ) );
    } catch ( Exception ex ) {
      MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ex );
      return response;
    }

    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  private JSONObject previewDocumentStructureAction( TransMeta transMeta ) {
    JSONObject response = new JSONObject();
    String missingConDetails = MongoDBHelper.validateRequestForFields( mongoDbOutputMeta );
    if ( !StringUtils.isEmpty( missingConDetails ) ) {
      return MongoDBHelper.errorResponse( response,
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS, missingConDetails ) );
    }

    try {
      MongoDbOutputUtilHelper mongoDbOutputUtilHelper = new MongoDbOutputUtilHelper();
      List<MongoDbOutputMeta.MongoField> mongoFields = mongoDbOutputMeta.getMongoFields();
      Map<String, String> displayDetails = mongoDbOutputUtilHelper.previewDocStructure( transMeta, mongoDbOutputMeta.getParentStepMeta().getName(), mongoFields, mongoDbOutputMeta.m_modifierUpdate );
      response.put( "windowTitle", displayDetails.get( "windowTitle" ) );
      response.put( "toDisplay", displayDetails.get( "toDisplay" )  );
    } catch ( Exception ex ) {
      MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message" ), ex );
      return response;
    }

    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  private JSONObject showIndexesAction( TransMeta transMeta ) {
    JSONObject response = new JSONObject();
    String missingConDetails = MongoDBHelper.validateRequestForFields( mongoDbOutputMeta );
    if ( !StringUtils.isEmpty( missingConDetails ) ) {
      return MongoDBHelper.errorResponse( response,
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
                    BaseMessages.getString( PKG, MISSING_CONN_DETAILS, missingConDetails ) );
    }

    String hostname = transMeta.environmentSubstitute( mongoDbOutputMeta.getHostnames() );
    String dbName = transMeta.environmentSubstitute( mongoDbOutputMeta.getDbName() );
    String collection = transMeta.environmentSubstitute( mongoDbOutputMeta.getCollection() );
    if ( !Utils.isEmpty( hostname ) ) {
      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbOutputMeta, transMeta, log );
        StringBuilder result = new StringBuilder();
        for ( String index : wrapper.getIndexInfo( dbName, collection ) ) {
          result.append( index ).append( "\n\n" );
        }

        response.put( "indexes", result.toString() );
      } catch ( Exception e ) {
        MongoDBHelper.errorResponse( response, BaseMessages.getString( PKG, "MongoDbOutputDialog.ErrorMessage.IndexPreview.Title" ), e );
        return response;
      }
    }

    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  private JSONArray setFieldResponse( List<ValueMetaInterface> fields ) {
    JSONArray jsonArray = new JSONArray();
    for ( ValueMetaInterface field : fields ) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put( "name", field.getName() );
      jsonArray.add( jsonObject );
    }
    return jsonArray;
  }

}
