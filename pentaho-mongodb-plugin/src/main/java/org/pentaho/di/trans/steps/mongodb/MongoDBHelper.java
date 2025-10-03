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
package org.pentaho.di.trans.steps.mongodb;

import org.json.simple.JSONObject;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.reporting.libraries.base.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MongoDBHelper {

  private static final Class<?> PKG = MongoDBHelper.class;
  public static final String ERROR_MESSAGE = "errorMessage";
  private static final String ERROR_LABEL = "errorLabel";
  private static final String MISSING_CONN_DETAILS = "MongoDb.ErrorMessage.MissingConnectionDetails";
  private static final String MISSING_CONN_DETAILS_TITLE = "MongoDb.ErrorMessage.MissingConnectionDetails.Title";
  private static final String UNABLE_TO_CONNECT =  "MongoDb.ErrorMessage.UnableToConnect";

  public JSONObject testConnectionAction( TransMeta transMeta, MongoDbMeta mongoDbMeta ) {
    JSONObject response = new JSONObject();

    if ( StringUtils.isEmpty( mongoDbMeta.getConnectionString() ) ) {
      return errorResponse( response,
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS, "Connection String" ) );
    }

    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, transMeta, transMeta.getLogChannel() );
      try {
        wrapper.getDatabaseNames();
      } finally {
        wrapper.dispose();
      }
    } catch ( Exception ex ) {
      errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT ), ex );
      response.put( "isValidConnection", false );
      return response;
    }

    response.put( "isValidConnection", true );
    response.put( "title", BaseMessages.getString( PKG, "MongoDb.SuccessMessage.SuccessConnectionDetails.Title" ) );
    response.put( "message", BaseMessages.getString( PKG, "MongoDb.SuccessMessage.SuccessConnectionDetails" ) );
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  public JSONObject getDBNamesAction( TransMeta transMeta, MongoDbMeta mongoDbMeta ) {
    JSONObject response = new JSONObject();
    List<String> dbNames;

    boolean isValidRequest = validateRequestForDbNamesAndCollections( response, mongoDbMeta );
    if ( !isValidRequest ) {
      return response;
    }

    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, transMeta, transMeta.getLogChannel() );
      try {
        dbNames = wrapper.getDatabaseNames();
      } finally {
        wrapper.dispose();
      }
    } catch ( Exception ex ) {
      return errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT ), ex );
    }

    response.put( "dbNames", dbNames );
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  public JSONObject getCollectionNamesAction( TransMeta transMeta, MongoDbMeta mongoDbMeta ) {
    JSONObject response = new JSONObject();
    Set<String> collectionNames;

    boolean isValidRequest = validateRequestForDbNamesAndCollections( response, mongoDbMeta );
    if ( !isValidRequest ) {
      return response;
    }

    if ( StringUtils.isEmpty( mongoDbMeta.getDbName() ) ) {
      errorResponse( response,
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
          BaseMessages.getString( PKG, MISSING_CONN_DETAILS, "database" ) );
      return response;
    }

    try {
      MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, transMeta, transMeta.getLogChannel() );
      try {
        collectionNames = wrapper.getCollectionsNames( transMeta.environmentSubstitute( mongoDbMeta.getDbName() ) );
      } finally {
        wrapper.dispose();
      }
    } catch ( Exception ex ) {
      return errorResponse( response, BaseMessages.getString( PKG, UNABLE_TO_CONNECT ), ex );
    }

    response.put( "collectionNames", new ArrayList<>( collectionNames ) );
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  public JSONObject getPreferencesAction() {
    JSONObject response = new JSONObject();
    List<String> preferences = new ArrayList<>();
    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      preferences.add( preference.getName() );
    }

    response.put( "preferences", preferences );
    response.put( StepInterface.ACTION_STATUS, StepInterface.SUCCESS_RESPONSE );
    return response;
  }

  public static JSONObject errorResponse( JSONObject response, String label, Exception ex ) {
    return errorResponse( response, label, ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage() );
  }

  public static JSONObject errorResponse( JSONObject response, String label, String message ) {
    response.put( ERROR_LABEL, label );
    response.put( ERROR_MESSAGE, message );
    response.put( StepInterface.ACTION_STATUS, StepInterface.FAILURE_RESPONSE );
    return response;
  }

  public static String validateRequestForFields( MongoDbMeta mongoDbMeta ) {
    String missingConDetails = "";
    if ( !mongoDbMeta.isUseConnectionString() ) {
      if ( StringUtils.isEmpty( mongoDbMeta.getHostnames() ) ) {
        missingConDetails += " host name(s)";
      }

      if ( StringUtils.isEmpty( mongoDbMeta.getDbName() ) ) {
        missingConDetails += " database";
      }

      if ( StringUtils.isEmpty( mongoDbMeta.getCollection() ) ) {
        missingConDetails += " collection";
      }
    } else {
      if ( StringUtils.isEmpty( mongoDbMeta.getConnectionString() ) ) {
        missingConDetails += " Connection string";
      }
    }

    return missingConDetails;
  }

  private boolean validateRequestForDbNamesAndCollections( JSONObject response, MongoDbMeta mongoDbMeta ) {
    if ( mongoDbMeta != null ) {
      boolean isConnectionDetailsEmpty = StringUtils.isEmpty( mongoDbMeta.getHostnames() ) && StringUtils.isEmpty( mongoDbMeta.getConnectionString() );
      if ( isConnectionDetailsEmpty ) {
        String errorString = mongoDbMeta.isUseConnectionString() ? "Connection String" : "Hostname";
        errorResponse( response,
            BaseMessages.getString( PKG, MISSING_CONN_DETAILS_TITLE ),
            BaseMessages.getString( PKG, MISSING_CONN_DETAILS, errorString ) );
        return false;
      }
    }

    return true;
  }
}
