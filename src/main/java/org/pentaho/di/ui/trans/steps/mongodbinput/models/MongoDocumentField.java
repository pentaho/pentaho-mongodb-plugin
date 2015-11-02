/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.ui.trans.steps.mongodbinput.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.util.AbstractModelList;

public class MongoDocumentField extends XulEventSourceAdapter {

  private String m_fieldName;
  private String m_fieldPath;
  private String m_kettleType;
  private List<String> m_indexedVals;
  private String m_arrayIndexInfo;
  private String m_occurenceFraction;
  private boolean m_disparateTypes;

  public String getFieldName() {
    return this.m_fieldName;
  }

  public void setFieldName( String name ) {
    this.m_fieldName = name;
  }

  public String getFieldPath() {
    return this.m_fieldPath;
  }

  public void setFieldPath( String path ) {
    this.m_fieldPath = path;
  }

  public String getKettleType() {
    return this.m_kettleType;
  }

  public void setKettleType( String type ) {
    this.m_kettleType = type;
  }

  public String getIndexedValues() {
    if ( !Const.isEmpty( m_indexedVals ) ) {
      return MongoDbInputData.indexedValsList( m_indexedVals );
    }
    return "";
  }

  public void setIndexedValues( String values ) {
    String[] vals = values.split( ",", -2 );
    m_indexedVals = Arrays.asList( vals );
  }

  public String getArrayIndexInfo() {
    return this.m_arrayIndexInfo;
  }

  public void setArrayIndexInfo( String info ) {
    this.m_arrayIndexInfo = info;
  }

  public String getOccurenceFraction() {
    return this.m_occurenceFraction;
  }

  public void setOccurenceFraction( String fraction ) {
    this.m_occurenceFraction = fraction;
  }

  public String getIsDisparateTypes() {
    return ( this.m_disparateTypes ? "Y" : "" );
  }

  public void setIsDisparateTypes( String bool ) {
    this.m_disparateTypes = "Y".equals( bool ) ? true : false;
  }

  protected void convertFrom( MongoField field ) {
    this.m_arrayIndexInfo = field.m_arrayIndexInfo;
    this.m_disparateTypes = field.m_disparateTypes;
    this.m_fieldName = field.m_fieldName;
    this.m_fieldPath = field.m_fieldPath;
    this.m_indexedVals = field.m_indexedVals;
    this.m_kettleType = field.m_kettleType;
    this.m_occurenceFraction = field.m_occurenceFraction;
  }

  protected void convertTo( MongoField field ) {
    field.m_arrayIndexInfo = this.m_arrayIndexInfo;
    field.m_disparateTypes = this.m_disparateTypes;
    field.m_fieldName = this.m_fieldName;
    field.m_fieldPath = this.m_fieldPath;
    field.m_indexedVals = this.m_indexedVals;
    field.m_kettleType = this.m_kettleType;
    field.m_occurenceFraction = this.m_occurenceFraction;
  }

  public static void convertList( List<MongoField> fields, AbstractModelList<MongoDocumentField> docFields ) {

    if ( fields == null || fields.isEmpty() ) {
      return;
    }
    for ( MongoField field : fields ) {
      MongoDocumentField docField = new MongoDocumentField();
      docField.convertFrom( field );
      docFields.add( docField );
    }
  }

  public static void trimList( List<MongoField> fields, AbstractModelList<MongoDocumentField> docFields ) {

    if ( fields == null || fields.isEmpty() ) {
      return;
    }
    for ( int i = fields.size() - 1; i >= 0; i-- ) {
      MongoField field = fields.get( i );
      for ( MongoDocumentField docField : docFields.asList() ) {
        if ( ( field.m_fieldName.equalsIgnoreCase( docField.m_fieldName ) ) && ( field.m_fieldPath
            .equalsIgnoreCase( docField.m_fieldPath ) ) ) {
          fields.remove( field );
          break;
        }
      }
    }
  }

  public static List<MongoField> convertFromList( AbstractModelList<MongoDocumentField> docFields ) {
    List<MongoField> fields = new ArrayList<MongoField>();

    if ( docFields == null || docFields.isEmpty() ) {
      return fields;
    }
    for ( MongoDocumentField docField : docFields ) {
      MongoField field = new MongoField();
      docField.convertTo( field );
      fields.add( field );
    }
    return fields;
  }
}
