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

package org.pentaho.mongo.wrapper.field;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class MongoArrayExpansion {
  protected static Class<?> PKG = MongoArrayExpansion.class; // for i18n purposes

  /**
   * The prefix of the full path that defines the expansion
   */
  public String m_expansionPath;

  /**
   * Subfield objects that handle the processing of the path after the expansion prefix
   */
  protected List<MongoField> m_subFields;

  private List<String> m_pathParts;
  private List<String> m_tempParts;

  public RowMetaInterface m_outputRowMeta;

  public MongoArrayExpansion( List<MongoField> subFields ) {
    m_subFields = subFields;
  }

  /**
   * Initialize this field by parsing the path etc.
   *
   * @throws KettleException if a problem occurs
   */
  public void init() throws KettleException {
    if ( Const.isEmpty( m_expansionPath ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.NoPathSet" ) ); //$NON-NLS-1$
    }
    if ( m_pathParts != null ) {
      return;
    }

    String expansionPath = MongoDbInputData.cleansePath( m_expansionPath );

    String[] temp = expansionPath.split( "\\." ); //$NON-NLS-1$
    m_pathParts = new ArrayList<String>();
    for ( String part : temp ) {
      m_pathParts.add( part );
    }

    if ( m_pathParts.get( 0 ).equals( "$" ) ) { //$NON-NLS-1$
      m_pathParts.remove( 0 ); // root record indicator
    } else if ( m_pathParts.get( 0 ).startsWith( "$[" ) ) { //$NON-NLS-1$

      // strip leading $ off of array
      String r = m_pathParts.get( 0 ).substring( 1, m_pathParts.get( 0 ).length() );
      m_pathParts.set( 0, r );
    }
    m_tempParts = new ArrayList<String>();

    // initialize the sub fields
    if ( m_subFields != null ) {
      for ( MongoField f : m_subFields ) {
        int outputIndex = m_outputRowMeta.indexOfValue( f.m_fieldName );
        f.init( outputIndex );
      }
    }
  }

  /**
   * Reset this field. Should be called prior to processing a new field value from the avro file
   *
   * @param space environment variables (values that environment variables resolve to cannot contain "."s)
   */
  public void reset( VariableSpace space ) {
    m_tempParts.clear();

    for ( String part : m_pathParts ) {
      m_tempParts.add( space.environmentSubstitute( part ) );
    }

    // reset sub fields
    for ( MongoField f : m_subFields ) {
      f.reset( space );
    }
  }

  protected Object[][] nullResult() {
    Object[][] result = new Object[1][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

    return result;
  }

  public Object[][] convertToKettleValue( BasicDBObject mongoObject, VariableSpace space ) throws KettleException {

    if ( mongoObject == null ) {
      return nullResult();
    }

    if ( m_tempParts.size() == 0 ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.MalformedPathRecord" ) ); //$NON-NLS-1$
    }

    String part = m_tempParts.remove( 0 );

    if ( part.charAt( 0 ) == '[' ) {
      // we're not expecting an array at this point - this document does not
      // contain our field(s)
      return nullResult();
    }

    if ( part.indexOf( '[' ) > 0 ) {
      String arrayPart = part.substring( part.indexOf( '[' ) );
      part = part.substring( 0, part.indexOf( '[' ) );

      // put the array section back into location zero
      m_tempParts.add( 0, arrayPart );
    }

    // part is a named field of this record
    Object fieldValue = mongoObject.get( part );
    if ( fieldValue == null ) {
      return nullResult();
    }

    if ( fieldValue instanceof BasicDBObject ) {
      return convertToKettleValue( ( (BasicDBObject) fieldValue ), space );
    }

    if ( fieldValue instanceof BasicDBList ) {
      return convertToKettleValue( ( (BasicDBList) fieldValue ), space );
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return nullResult();
  }

  public Object[][] convertToKettleValue( BasicDBList mongoList, VariableSpace space ) throws KettleException {

    if ( mongoList == null ) {
      return nullResult();
    }

    if ( m_tempParts.size() == 0 ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.MalformedPathArray" ) ); //$NON-NLS-1$
    }

    String part = m_tempParts.remove( 0 );
    if ( !( part.charAt( 0 ) == '[' ) ) {
      // we're expecting an array at this point - this document does not
      // contain our field
      return nullResult();
    }

    String index = part.substring( 1, part.indexOf( ']' ) );

    if ( part.indexOf( ']' ) < part.length() - 1 ) {
      // more dimensions to the array
      part = part.substring( part.indexOf( ']' ) + 1, part.length() );
      m_tempParts.add( 0, part );
    }

    if ( index.equals( "*" ) ) { //$NON-NLS-1$
      // start the expansion - we delegate conversion to our subfields
      Object[][] result = new Object[mongoList.size()][m_outputRowMeta.size() + RowDataUtil.OVER_ALLOCATE_SIZE];

      for ( int i = 0; i < mongoList.size(); i++ ) {
        Object element = mongoList.get( i );

        for ( int j = 0; j < m_subFields.size(); j++ ) {
          MongoField sf = m_subFields.get( j );
          sf.reset( space );

          // what have we got?
          if ( element instanceof BasicDBObject ) {
            result[i][sf.m_outputIndex] = sf.convertToKettleValue( (BasicDBObject) element );
          } else if ( element instanceof BasicDBList ) {
            result[i][sf.m_outputIndex] = sf.convertToKettleValue( (BasicDBList) element );
          } else {
            // assume a primitive
            result[i][sf.m_outputIndex] = sf.getKettleValue( element );
          }
        }
      }

      return result;
    } else {
      int arrayI = 0;
      try {
        arrayI = Integer.parseInt( index.trim() );
      } catch ( NumberFormatException e ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index ) ); //$NON-NLS-1$
      }

      if ( arrayI >= mongoList.size() || arrayI < 0 ) {
        // index is out of bounds
        return nullResult();
      }

      Object element = mongoList.get( arrayI );

      if ( element == null ) {
        return nullResult();
      }

      if ( element instanceof BasicDBObject ) {
        return convertToKettleValue( ( (BasicDBObject) element ), space );
      }

      if ( element instanceof BasicDBList ) {
        return convertToKettleValue( ( (BasicDBList) element ), space );
      }

      // must mean we have a primitive here, but we're expecting to process
      // more
      // path so this doesn't match us - return null
      return nullResult();
    }
  }
}
