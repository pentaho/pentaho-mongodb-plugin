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

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
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
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.w3c.dom.Node;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 8-apr-2011
 * 
 * @author matt
 * @since 4.2.0-M1
 */
@Step( id = "MongoDbInput", image = "mongodb-input.svg", name = "MongoDB Input",
    description = "Reads from a Mongo DB collection", documentationUrl = "http://wiki.pentaho.com/display/EAI/MongoDB+Input", categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "MongoDbInput.Injection.", groups = ( "FIELDS" ) )
public class MongoDbInputMeta extends MongoDbMeta {
  protected static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes

  @Injection( name = "JSON_OUTPUT_FIELD" )
  private String jsonFieldName;
  @Injection( name = "JSON_FIELD" )
  private String fields;

  @Injection( name = "JSON_QUERY" )
  private String jsonQuery;

  @Injection( name = "AGG_PIPELINE" )
  private boolean m_aggPipeline = false;

  @Injection( name = "OUTPUT_JSON" )
  private boolean m_outputJson = true;

  @InjectionDeep
  private List<MongoField> m_fields;

  @Injection( name = "EXECUTE_FOR_EACH_ROW" )
  private boolean m_executeForEachIncomingRow = false;

  public void setMongoFields( List<MongoField> fields ) {
    m_fields = fields;
  }

  public List<MongoField> getMongoFields() {
    return m_fields;
  }

  public void setExecuteForEachIncomingRow( boolean e ) {
    m_executeForEachIncomingRow = e;
  }

  public boolean getExecuteForEachIncomingRow() {
    return m_executeForEachIncomingRow;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  @Override
  public Object clone() {
    MongoDbInputMeta retval = (MongoDbInputMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      setHostnames( XMLHandler.getTagValue( stepnode, "hostname" ) ); //$NON-NLS-1$ 
      setPort( XMLHandler.getTagValue( stepnode, "port" ) ); //$NON-NLS-1$ 
      setDbName( XMLHandler.getTagValue( stepnode, "db_name" ) ); //$NON-NLS-1$
      fields = XMLHandler.getTagValue( stepnode, "fields_name" ); //$NON-NLS-1$
      setCollection( XMLHandler.getTagValue( stepnode, "collection" ) ); //$NON-NLS-1$
      jsonFieldName = XMLHandler.getTagValue( stepnode, "json_field_name" ); //$NON-NLS-1$
      jsonQuery = XMLHandler.getTagValue( stepnode, "json_query" ); //$NON-NLS-1$
      setAuthenticationDatabaseName( XMLHandler.getTagValue( stepnode, "auth_database" ) ); //$NON-NLS-1$
      setAuthenticationUser( XMLHandler.getTagValue( stepnode, "auth_user" ) ); //$NON-NLS-1$
      setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode,
          "auth_password" ) ) ); //$NON-NLS-1$

      setAuthenticationMechanism( XMLHandler.getTagValue( stepnode, "auth_mech" ) );
      boolean kerberos = false;
      String useKerberos = XMLHandler.getTagValue( stepnode, "auth_kerberos" ); //$NON-NLS-1$
      if ( !Const.isEmpty( useKerberos ) ) {
        kerberos = useKerberos.equalsIgnoreCase( "Y" );
      }
      setUseKerberosAuthentication( kerberos );

      setConnectTimeout( XMLHandler.getTagValue( stepnode, "connect_timeout" ) ); //$NON-NLS-1$
      setSocketTimeout( XMLHandler.getTagValue( stepnode, "socket_timeout" ) ); //$NON-NLS-1$
      setReadPreference( XMLHandler.getTagValue( stepnode, "read_preference" ) ); //$NON-NLS-1$

      m_outputJson = true; // default to true for backwards compatibility
      String outputJson = XMLHandler.getTagValue( stepnode, "output_json" ); //$NON-NLS-1$
      if ( !Const.isEmpty( outputJson ) ) {
        m_outputJson = outputJson.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      setUseAllReplicaSetMembers( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_all_replica_members" ) ) ); //$NON-NLS-1$

      String queryIsPipe = XMLHandler.getTagValue( stepnode, "query_is_pipeline" ); //$NON-NLS-1$
      if ( !Const.isEmpty( queryIsPipe ) ) {
        m_aggPipeline = queryIsPipe.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      String executeForEachR = XMLHandler.getTagValue( stepnode, "execute_for_each_row" );
      if ( !Const.isEmpty( executeForEachR ) ) {
        m_executeForEachIncomingRow = executeForEachR.equalsIgnoreCase( "Y" );
      }

      Node mongo_fields = XMLHandler.getSubNode( stepnode, "mongo_fields" ); //$NON-NLS-1$
      if ( mongo_fields != null && XMLHandler.countNodes( mongo_fields, "mongo_field" ) > 0 ) { //$NON-NLS-1$
        int nrfields = XMLHandler.countNodes( mongo_fields, "mongo_field" ); //$NON-NLS-1$

        m_fields = new ArrayList<MongoField>();
        for ( int i = 0; i < nrfields; i++ ) {
          Node fieldNode = XMLHandler.getSubNodeByNr( mongo_fields, "mongo_field", i ); //$NON-NLS-1$

          MongoField newField = new MongoField();
          newField.m_fieldName = XMLHandler.getTagValue( fieldNode, "field_name" ); //$NON-NLS-1$
          newField.m_fieldPath = XMLHandler.getTagValue( fieldNode, "field_path" ); //$NON-NLS-1$
          newField.m_kettleType = XMLHandler.getTagValue( fieldNode, "field_type" ); //$NON-NLS-1$
          String indexedVals = XMLHandler.getTagValue( fieldNode, "indexed_vals" ); //$NON-NLS-1$
          if ( indexedVals != null && indexedVals.length() > 0 ) {
            newField.m_indexedVals = MongoDbInputData.indexedValsList( indexedVals );
          }

          m_fields.add( newField );
        }
      }

      String tags = XMLHandler.getTagValue( stepnode, "tag_sets" ); //$NON-NLS-1$
      if ( !Const.isEmpty( tags ) ) {
        setReadPrefTagSets( new ArrayList<String>() );

        String[] parts = tags.split( "#@#" ); //$NON-NLS-1$
        for ( String p : parts ) {
          getReadPrefTagSets().add( p.trim() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "MongoDbInputMeta.Exception.UnableToLoadStepInfo" ), e ); //$NON-NLS-1$
    }
  }

  @Override
  public void setDefault() {
    setHostnames( "localhost" ); //$NON-NLS-1$
    setPort( "27017" ); //$NON-NLS-1$
    setDbName( "db" ); //$NON-NLS-1$
    setCollection( "collection" ); //$NON-NLS-1$
    jsonFieldName = "json"; //$NON-NLS-1$
  }

  @SuppressWarnings( "deprecation" )
  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space ) throws KettleStepException {

    if ( m_outputJson || m_fields == null || m_fields.size() == 0 ) {
      ValueMetaInterface jsonValueMeta = new ValueMeta( jsonFieldName, ValueMetaInterface.TYPE_STRING );
      jsonValueMeta.setOrigin( origin );
      rowMeta.addValueMeta( jsonValueMeta );
    } else {
      for ( MongoField f : m_fields ) {
        ValueMetaInterface vm = new ValueMeta();
        vm.setName( f.m_fieldName );
        vm.setOrigin( origin );
        vm.setType( ValueMeta.getType( f.m_kettleType ) );
        if ( f.m_indexedVals != null ) {
          vm.setIndex( f.m_indexedVals.toArray() ); // indexed values
        }
        rowMeta.addValueMeta( vm );
      }
    }
  }

  protected String tagSetsToString() {
    if ( getReadPrefTagSets() != null && getReadPrefTagSets().size() > 0 ) {
      StringBuilder builder = new StringBuilder();
      for ( int i = 0; i < getReadPrefTagSets().size(); i++ ) {
        String s = getReadPrefTagSets().get( i );
        s = s.trim();
        if ( !s.startsWith( "{" ) ) { //$NON-NLS-1$
          s = "{" + s; //$NON-NLS-1$
        }
        if ( !s.endsWith( "}" ) ) { //$NON-NLS-1$
          s += "}"; //$NON-NLS-1$
        }

        builder.append( s );
        if ( i != getReadPrefTagSets().size() - 1 ) {
          builder.append( "#@#" ); //$NON-NLS-1$
        }
      }
      return builder.toString();
    }
    return null;
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "hostname", getHostnames() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "port", getPort() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "use_all_replica_members", getUseAllReplicaSetMembers() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "db_name", getDbName() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "fields_name", fields ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "collection", getCollection() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "json_field_name", jsonFieldName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "json_query", jsonQuery ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_database", getAuthenticationDatabaseName() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_user", getAuthenticationUser() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_password", //$NON-NLS-1$
            Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) ) );
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_mech", getAuthenticationMechanism() ) );
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "auth_kerberos", getUseKerberosAuthentication() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "connect_timeout", getConnectTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "socket_timeout", getSocketTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "read_preference", getReadPreference() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "output_json", m_outputJson ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "query_is_pipeline", m_aggPipeline ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
        XMLHandler.addTagValue( "execute_for_each_row", m_executeForEachIncomingRow ) ); //$NON-NLS-1$

    if ( m_fields != null && m_fields.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$

      for ( MongoField f : m_fields ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$

        retval.append( "\n        " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "field_name", f.m_fieldName ) ); //$NON-NLS-1$
        retval.append( "\n        " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "field_path", f.m_fieldPath ) ); //$NON-NLS-1$
        retval.append( "\n        " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "field_type", f.m_kettleType ) ); //$NON-NLS-1$
        if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
          retval.append( "\n        " ).append( //$NON-NLS-1$
              XMLHandler.addTagValue( "indexed_vals", //$NON-NLS-1$
                  MongoDbInputData.indexedValsList( f.m_indexedVals ) ) );
        }
        retval.append( "\n      " ).append( XMLHandler.closeTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    String tags = tagSetsToString();
    if ( !Const.isEmpty( tags ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "tag_sets", tags ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return retval.toString();
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      setHostnames( rep.getStepAttributeString( id_step, "hostname" ) ); //$NON-NLS-1$
      setPort( rep.getStepAttributeString( id_step, "port" ) ); //$NON-NLS-1$
      setUseAllReplicaSetMembers( rep.getStepAttributeBoolean( id_step, 0, "use_all_replica_members" ) ); //$NON-NLS-1$
      setDbName( rep.getStepAttributeString( id_step, "db_name" ) ); //$NON-NLS-1$
      fields = rep.getStepAttributeString( id_step, "fields_name" ); //$NON-NLS-1$
      setCollection( rep.getStepAttributeString( id_step, "collection" ) ); //$NON-NLS-1$
      jsonFieldName = rep.getStepAttributeString( id_step, "json_field_name" ); //$NON-NLS-1$
      jsonQuery = rep.getStepAttributeString( id_step, "json_query" ); //$NON-NLS-1$

      setAuthenticationDatabaseName( rep.getStepAttributeString( id_step, "auth_database" ) ); //$NON-NLS-1$
      setAuthenticationMechanism( rep.getStepAttributeString( id_step, "auth_mech" ) );
      setAuthenticationUser( rep.getStepAttributeString( id_step, "auth_user" ) ); //$NON-NLS-1$
      setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step,
          "auth_password" ) ) ); //$NON-NLS-1$
      setUseKerberosAuthentication( rep.getStepAttributeBoolean( id_step, "auth_kerberos" ) ); //$NON-NLS-1$
      setConnectTimeout( rep.getStepAttributeString( id_step, "connect_timeout" ) ); //$NON-NLS-1$
      setSocketTimeout( rep.getStepAttributeString( id_step, "socket_timeout" ) ); //$NON-NLS-1$
      setReadPreference( rep.getStepAttributeString( id_step, "read_preference" ) ); //$NON-NLS-1$

      m_outputJson = rep.getStepAttributeBoolean( id_step, 0, "output_json" ); //$NON-NLS-1$
      m_aggPipeline = rep.getStepAttributeBoolean( id_step, "query_is_pipeline" ); //$NON-NLS-1$
      m_executeForEachIncomingRow = rep.getStepAttributeBoolean( id_step, "execute_for_each_row" ); //$NON-NLS-1$

      int nrfields = rep.countNrStepAttributes( id_step, "field_name" ); //$NON-NLS-1$
      if ( nrfields > 0 ) {
        m_fields = new ArrayList<MongoField>();

        for ( int i = 0; i < nrfields; i++ ) {
          MongoField newField = new MongoField();

          newField.m_fieldName = rep.getStepAttributeString( id_step, i, "field_name" ); //$NON-NLS-1$
          newField.m_fieldPath = rep.getStepAttributeString( id_step, i, "field_path" ); //$NON-NLS-1$
          newField.m_kettleType = rep.getStepAttributeString( id_step, i, "field_type" ); //$NON-NLS-1$
          String indexedVals = rep.getStepAttributeString( id_step, i, "indexed_vals" ); //$NON-NLS-1$
          if ( indexedVals != null && indexedVals.length() > 0 ) {
            newField.m_indexedVals = MongoDbInputData.indexedValsList( indexedVals );
          }

          m_fields.add( newField );
        }
      }

      String tags = rep.getStepAttributeString( id_step, "tag_sets" ); //$NON-NLS-1$
      if ( !Const.isEmpty( tags ) ) {
        setReadPrefTagSets( new ArrayList<String>() );

        String[] parts = tags.split( "#@#" ); //$NON-NLS-1$
        for ( String p : parts ) {
          getReadPrefTagSets().add( p.trim() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "MongoDbInputMeta.Exception.UnexpectedErrorWhileReadingStepInfo" ), e ); //$NON-NLS-1$
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "hostname", getHostnames() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "port", getPort() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "use_all_replica_members", getUseAllReplicaSetMembers() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "db_name", getDbName() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "fields_name", fields ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "collection", getCollection() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "json_field_name", jsonFieldName ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "json_query", jsonQuery ); //$NON-NLS-1$

      rep.saveStepAttribute( id_transformation, id_step, "auth_database", //$NON-NLS-1$
              getAuthenticationDatabaseName() );
      rep.saveStepAttribute( id_transformation, id_step, "auth_user", //$NON-NLS-1$
          getAuthenticationUser() );
      rep.saveStepAttribute( id_transformation, id_step, "auth_password", //$NON-NLS-1$
          Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) );
      rep.saveStepAttribute( id_transformation, id_step, "auth_mech", getAuthenticationMechanism() );
      rep.saveStepAttribute( id_transformation, id_step, "auth_kerberos", getUseKerberosAuthentication() );
      rep.saveStepAttribute( id_transformation, id_step, "connect_timeout", getConnectTimeout() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "socket_timeout", getSocketTimeout() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "read_preference", getReadPreference() ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, 0, "output_json", //$NON-NLS-1$
          m_outputJson );
      rep.saveStepAttribute( id_transformation, id_step, 0, "query_is_pipeline", //$NON-NLS-1$
          m_aggPipeline );
      rep.saveStepAttribute( id_transformation, id_step, 0, "execute_for_each_row", //$NON-NLS-1$
          m_executeForEachIncomingRow );

      if ( m_fields != null && m_fields.size() > 0 ) {
        for ( int i = 0; i < m_fields.size(); i++ ) {
          MongoField f = m_fields.get( i );

          rep.saveStepAttribute( id_transformation, id_step, i, "field_name", //$NON-NLS-1$
              f.m_fieldName );
          rep.saveStepAttribute( id_transformation, id_step, i, "field_path", //$NON-NLS-1$
              f.m_fieldPath );
          rep.saveStepAttribute( id_transformation, id_step, i, "field_type", //$NON-NLS-1$
              f.m_kettleType );
          if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
            String indexedVals = MongoDbInputData.indexedValsList( f.m_indexedVals );

            rep.saveStepAttribute( id_transformation, id_step, i, "indexed_vals", indexedVals ); //$NON-NLS-1$
          }
        }
      }

      String tags = tagSetsToString();
      if ( !Const.isEmpty( tags ) ) {
        rep.saveStepAttribute( id_transformation, id_step, "tag_sets", tags ); //$NON-NLS-1$
      }
    } catch ( KettleException e ) {
      throw new KettleException(
          BaseMessages.getString( PKG, "MongoDbInputMeta.Exception.UnableToSaveStepInfo" ) + id_step, e ); //$NON-NLS-1$
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
      Trans trans ) {
    return new MongoDbInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MongoDbInputData();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {
    // TODO add checks
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fields;
  }

  /**
   * @param fields a field name to set
   */
  public void setFieldsName( String fields ) {
    this.fields = fields;
  }

  /**
   * @return the jsonFieldName
   */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /**
   * @param jsonFieldName
   *          the jsonFieldName to set
   */
  public void setJsonFieldName( String jsonFieldName ) {
    this.jsonFieldName = jsonFieldName;
  }

  /**
   * @return the jsonQuery
   */
  public String getJsonQuery() {
    return jsonQuery;
  }

  /**
   * @param jsonQuery
   *          the jsonQuery to set
   */
  public void setJsonQuery( String jsonQuery ) {
    this.jsonQuery = jsonQuery;
  }

  /**
   * Set whether to output just a single field as JSON
   * 
   * @param outputJson
   *          true if a single field containing JSON is to be output
   */
  public void setOutputJson( boolean outputJson ) {
    m_outputJson = outputJson;
  }

  /**
   * Get whether to output just a single field as JSON
   * 
   * @return true if a single field containing JSON is to be output
   */
  public boolean getOutputJson() {
    return m_outputJson;
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   * 
   * @param q
   *          true if the supplied query is a pipeline specification
   */
  public void setQueryIsPipeline( boolean q ) {
    m_aggPipeline = q;
  }

  /**
   * Get whether the supplied query is actually a pipeline specification
   * 
   * @true true if the supplied query is a pipeline specification
   */
  public boolean getQueryIsPipeline() {
    return m_aggPipeline;
  }
}
