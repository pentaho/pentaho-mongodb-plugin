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

package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.DBObject;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.mongo.MongoDbException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDbOutputHelper {

  private static final Class<?> PKG = MongoDbOutputHelper.class;
  private enum Element {
    OPEN_BRACE, CLOSE_BRACE, OPEN_BRACKET, CLOSE_BRACKET, COMMA
  }

  public Map<String, String> previewDocStructure( TransMeta transMeta, String stepName, List<MongoDbOutputMeta.MongoField> mongoFields, boolean updateSelection ) throws KettleException, MongoDbException {
    Map<String, String> displayDetails = new HashMap<>();
    String windowTitle = getString( "MongoDbOutputDialog.PreviewDocStructure.Title" );
    String toDisplay = "";

    if ( mongoFields == null || mongoFields.isEmpty() ) {
      return displayDetails;
    }

    // Try and get metadata on incoming fields
    RowMetaInterface actualR = null;
    RowMetaInterface r;
    boolean gotGenuineRowMeta = false;
    try {
      actualR = transMeta.getPrevStepFields( stepName );
      gotGenuineRowMeta = true;
    } catch ( KettleException e ) {
      // don't complain if we can't
    }
    r = new RowMeta();

    Object[] dummyRow = new Object[mongoFields.size()];
    int i = 0;
    // Initialize Variable space to allow for environment substitution during doc preview.
    VariableSpace vs = new Variables();
    vs.initializeVariablesFrom( transMeta );
    boolean hasTopLevelJSONDocInsert = MongoDbOutputData.scanForInsertTopLevelJSONDoc( mongoFields );

    for ( MongoDbOutputMeta.MongoField field : mongoFields ) {
      field.init( vs );
      // set up dummy row meta
      ValueMetaInterface vm = ValueMetaFactory.createValueMeta( ValueMetaInterface.TYPE_STRING );
      vm.setName( field.environUpdatedFieldName );
      r.addValueMeta( vm );

      String val = ""; //$NON-NLS-1$
      if ( gotGenuineRowMeta && actualR.indexOfValue( field.environUpdatedFieldName ) >= 0 ) {
        val = getMongoDataTypeDocument( field, actualR );
      } else {
        val = "<value>"; //$NON-NLS-1$
      }

      dummyRow[i++] = val;
    }

    MongoDbOutputData.MongoTopLevel topLevelStruct = MongoDbOutputData.checkTopLevelConsistency( mongoFields, vs );
    for ( MongoDbOutputMeta.MongoField m : mongoFields ) {
      m.m_modifierOperationApplyPolicy = "Insert&Update"; //$NON-NLS-1$
    }

    if ( !updateSelection ) {
      DBObject result = MongoDbOutputData.kettleRowToMongo( mongoFields, r, dummyRow, topLevelStruct, hasTopLevelJSONDocInsert );
      if ( result != null ) {
        toDisplay = prettyPrintDocStructure( result.toString() );
      }
    } else {
      DBObject query = MongoDbOutputData.getQueryObject( mongoFields, r, dummyRow, vs, topLevelStruct );
      DBObject modifier = new MongoDbOutputData().getModifierUpdateObject( mongoFields, r, dummyRow, vs, topLevelStruct );
      toDisplay = getDisplayStringForUpdate( query, modifier );
      windowTitle = getString( "MongoDbOutputDialog.PreviewModifierUpdate.Title" ); //$NON-NLS-1$
    }

    displayDetails.put( "windowTitle", windowTitle );
    displayDetails.put( "toDisplay", toDisplay );
    return displayDetails;
  }

  /**
   * Format JSON document structure for printing to the preview dialog
   *
   * @param toFormat the document to format
   * @return a String containing the formatted document structure
   */
  public String prettyPrintDocStructure( String toFormat ) {
    StringBuilder result = new StringBuilder();
    int indent = 0;
    String source = removeWhiteSpacesAroundCommas( toFormat );
    Element next = Element.OPEN_BRACE;

    while ( !source.isEmpty() ) {
      source = source.trim();
      String toIndent = ""; //$NON-NLS-1$

      Map<String, Object> setUpValues = setMinIndexAndNextElementAndTargetChar( source, next );
      int minIndex = (Integer) setUpValues.get( "minIndex" );
      char targetChar = (Character) setUpValues.get( "targetChar" );
      next = (Element) setUpValues.get( "next" );

      if ( minIndex == 0 ) {
        if ( next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET ) {
          indent -= 2;
        }

        pad( result, indent );

        String comma = ""; //$NON-NLS-1$
        int offset = 1;
        if ( source.length() >= 2 && source.charAt( 1 ) == ',' ) {
          comma = ","; //$NON-NLS-1$
          offset = 2;
        }
        result.append( targetChar ).append( comma ).append( "\n" ); //$NON-NLS-1$
        source = source.substring( offset );
      } else {
        pad( result, indent );
        Map<String, String> sourceAndToIndent = getSourceAndToIndentForNonMinIndex( next, source, minIndex );
        toIndent = sourceAndToIndent.get( "toIndent" );
        source = sourceAndToIndent.get( "source" );
        result.append( toIndent.trim() ).append( "\n" ); //$NON-NLS-1$
      }

      if ( next == Element.OPEN_BRACE || next == Element.OPEN_BRACKET ) {
        indent += 2;
      }
    }

    return result.toString();
  }

  private String removeWhiteSpacesAroundCommas( String toFormat ) {
    StringBuilder sourceBuilder = new StringBuilder();
    boolean lastWasComma = false;
    for ( char c : toFormat.toCharArray() ) {
      if ( c == ',' ) {
        if ( !lastWasComma ) {
          sourceBuilder.append( c );
        }
        lastWasComma = true;
      } else if ( !Character.isWhitespace( c ) ) {
        sourceBuilder.append( c );
        lastWasComma = false;
      }
    }
    return sourceBuilder.toString();
  }

  private Map<String, String> getSourceAndToIndentForNonMinIndex( Element next, String source, int minIndex ) {
    Map<String, String> result = new HashMap<>();
    String toIndent;
    if ( next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET ) {
      toIndent = source.substring( 0, minIndex );
      source = source.substring( minIndex );
    } else {
      toIndent = source.substring( 0, minIndex + 1 );
      source = source.substring( minIndex + 1 );
    }

    result.put( "toIndent", toIndent );
    result.put( "source", source );
    return result;
  }

  private Map<String, Object> setMinIndexAndNextElementAndTargetChar( String source, Element next ) {
    Map<String, Object> result = new HashMap<>();
    int minIndex = Integer.MAX_VALUE;
    char targetChar = '{';
    if ( source.indexOf( '{' ) > -1 ) {
      next = Element.OPEN_BRACE;
      minIndex = source.indexOf( '{' );
    }

    if ( source.indexOf( '}' ) > -1 && source.indexOf( '}' ) < minIndex ) {
      next = Element.CLOSE_BRACE;
      minIndex = source.indexOf( '}' );
      targetChar = '}';
    }

    if ( source.indexOf( '[' ) > -1 && source.indexOf( '[' ) < minIndex ) {
      next = Element.OPEN_BRACKET;
      minIndex = source.indexOf( '[' );
      targetChar = '[';
    }

    if ( source.indexOf( ']' ) > -1 && source.indexOf( ']' ) < minIndex ) {
      next = Element.CLOSE_BRACKET;
      minIndex = source.indexOf( ']' );
      targetChar = ']';
    }

    if ( source.indexOf( ',' ) > -1 && source.indexOf( ',' ) < minIndex ) {
      next = Element.COMMA;
      minIndex = source.indexOf( ',' );
      targetChar = ',';
    }

    result.put( "minIndex", minIndex );
    result.put( "next", next );
    result.put( "targetChar", targetChar );
    return result;
  }

  private String getDisplayStringForUpdate( DBObject query, DBObject modifier ) {
    return getString( "MongoDbOutputDialog.PreviewModifierUpdate.Heading1" ) //$NON-NLS-1$
        + ":\n\n" //$NON-NLS-1$
        +  ( query != null ? prettyPrintDocStructure( query.toString() ) : "" )
        + getString( "MongoDbOutputDialog.PreviewModifierUpdate.Heading2" ) //$NON-NLS-1$
        + ":\n\n" //$NON-NLS-1$
        + ( modifier !=  null ? prettyPrintDocStructure( modifier.toString() ) : "" );
  }

  private String getMongoDataTypeDocument( MongoDbOutputMeta.MongoField field, RowMetaInterface actualR ) {
    int index = actualR.indexOfValue( field.environUpdatedFieldName );
    String val;
    switch ( actualR.getValueMeta( index ).getType() ) {
      case ValueMetaInterface.TYPE_STRING:
        if ( field.m_JSON ) {
          if ( !field.m_useIncomingFieldNameAsMongoFieldName && Utils.isEmpty( field.environUpdateMongoDocPath ) ) {
            // we will actually have to parse some kind of JSON doc
            // here in the case where the matching doc/doc to be inserted is
            // a full top-level incoming JSON doc
            val = "{\"IncomingJSONDoc\" : \"<document content>\"}"; //$NON-NLS-1$
          } else {
            val = "<JSON sub document>"; //$NON-NLS-1$
            // turn this off for the purpose of doc structure
            // visualization so that we don't screw up for the
            // lack of a real JSON doc to parse :-)
            field.m_JSON = false;
          }
        } else {
          val = "<string val>"; //$NON-NLS-1$
        }
        break;
      case ValueMetaInterface.TYPE_INTEGER:
        val = "<integer val>"; //$NON-NLS-1$
        break;
      case ValueMetaInterface.TYPE_NUMBER:
        val = "<number val>"; //$NON-NLS-1$
        break;
      case ValueMetaInterface.TYPE_BOOLEAN:
        val = "<bool val>"; //$NON-NLS-1$
        break;
      case ValueMetaInterface.TYPE_DATE:
        val = "<date val>"; //$NON-NLS-1$
        break;
      case ValueMetaInterface.TYPE_BINARY:
        val = "<binary val>"; //$NON-NLS-1$
        break;
      default:
        val = "<unsupported value type>"; //$NON-NLS-1$
    }

    return val;
  }

  private void pad( StringBuilder toPad, int numBlanks ) {
    toPad.append( " ".repeat( Math.max( 0, numBlanks ) ) );
  }

  private String getString( String key ) {
    return BaseMessages.getString( PKG, key );
  }
}
