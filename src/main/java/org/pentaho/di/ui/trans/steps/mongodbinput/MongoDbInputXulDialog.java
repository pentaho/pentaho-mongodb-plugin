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

/**
 *
 */

package org.pentaho.di.ui.trans.steps.mongodbinput;

import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Vector;

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.step.BaseStepGenericXulDialog;
import org.pentaho.di.ui.trans.steps.mongodbinput.models.MongoDbModel;
import org.pentaho.di.ui.trans.steps.mongodbinput.models.MongoDocumentField;
import org.pentaho.di.ui.trans.steps.mongodbinput.models.MongoTag;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulSettingsManager;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.swing.SwingBindingFactory;
import org.pentaho.ui.xul.swing.SwingXulLoader;
import org.pentaho.ui.xul.swing.SwingXulRunner;

/**
 * @author gmoran
 * @date 01/28/2013
 */
public class MongoDbInputXulDialog extends BaseStepGenericXulDialog {

  private MongoDbModel model;

  private Binding databaseBinding;
  private Binding collectionBinding;

  protected static BindingConvertor<String, Boolean> emptyStringBinding = new IsEmptyStringToBooleanConvertor();
  protected static BindingConvertor<int[], Boolean> selectedItemsBinding = new SelectedToBooleanConvertor();
  protected static BindingConvertor<Collection<MongoTag>, Boolean> rowCountBinding =
      new RowCountToBooleanConvertor<MongoTag>();

  public MongoDbInputXulDialog( Object parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname ) {
    super( "org/pentaho/di/ui/trans/steps/mongodbinput/xul/mongodb_input.xul", parent, baseStepMeta, transMeta,
        stepname );
  }

  public XulSettingsManager getSettingsManager() {
    return null;
  }

  public ResourceBundle getResourceBundle() {
    return null;
  }

  public void init() {

    model = new MongoDbModel( (MongoDbInputMeta) stepMeta.getStepMetaInterface() );

    try {
      bf.setBindingType( Binding.Type.ONE_WAY );

      bf.createBinding( model, "possibleReadPreferences", "readPreference", "elements" ).fireSourceChanged();

      bf.createBinding( "hostName", "value", "getDbs", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getTags", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getFields", "disabled", emptyStringBinding ).fireSourceChanged();

      databaseBinding = bf.createBinding( model, "dbNames", "database", "elements" );
      databaseBinding.fireSourceChanged();

      collectionBinding = bf.createBinding( model, "collections", "collection", "elements" );
      collectionBinding.fireSourceChanged();

      // this controls enabling the external preview function ...
      bf.createBinding( model.getFields(), "children", this, "previewState" ).fireSourceChanged();

      bf.setBindingType( Binding.Type.BI_DIRECTIONAL );

      bf.createBinding( model, "hostnames", "hostName", "value" ).fireSourceChanged();
      bf.createBinding( model, "port", "port", "value" ).fireSourceChanged();
      bf.createBinding( model, "jsonQuery", "jsonQuery", "value" ).fireSourceChanged();
      bf.createBinding( model, "authenticationDatabaseName", "authDatabase", "value" ).fireSourceChanged();
      bf.createBinding( model, "authenticationPassword", "authPassword", "value" ).fireSourceChanged();
      bf.createBinding( model, "authenticationUser", "authUser", "value" ).fireSourceChanged();
      bf.createBinding( model, "fieldsName", "fieldsQuery", "value" ).fireSourceChanged();
      bf.createBinding( model, "socketTimeout", "socketTimeout", "value" ).fireSourceChanged();
      bf.createBinding( model, "connectTimeout", "connectionTimeout", "value" ).fireSourceChanged();

      bf.createBinding( model, "queryIsPipeline", "isAggPipeline", "checked" ).fireSourceChanged();
      bf.createBinding( model, "useAllReplicaMembers", "isUseAllReplicas", "checked" ).fireSourceChanged();

      bf.createBinding( model, "useKerberosAuthentication", "isUseKerberosAuthentication", "checked" )
          .fireSourceChanged();
      bf.createBinding( "isUseKerberosAuthentication", "checked", "authPassword", "disabled" ).fireSourceChanged();

      bf.createBinding( model, "dbName", "database", "selectedItem" ).fireSourceChanged();
      bf.createBinding( model, "collection", "collection", "selectedItem" ).fireSourceChanged();
      bf.createBinding( model, "readPreference", "readPreference", "selectedItem" ).fireSourceChanged();

      bf.createBinding( "isAggPipeline", "checked", "fieldsQuery", "disabled" ).fireSourceChanged();

      Binding fieldsBinding = bf.createBinding( model.getFields(), "children", "fieldsTable", "elements" );
      fieldsBinding.fireSourceChanged();

      Binding tagsBinding = bf.createBinding( model.getTags(), "children", "tagsTable", "elements" );
      tagsBinding.fireSourceChanged();

      bf.setBindingType( Binding.Type.ONE_WAY );

      bf.createBinding( "tagsTable", "absoluteSelectedRows", "joinTags", "disabled", selectedItemsBinding )
          .fireSourceChanged();
      bf.createBinding( model.getTags(), "children", "testTags", "disabled", rowCountBinding ).fireSourceChanged();

    } catch ( Exception e ) {
      log.logError( "Error creating bindings for dialog. This dialog will not be available", e );

    }
  }

  /**
   * Because we are using an AbstractModelList as our fields container, we need
   * to bind the validate() method here, instead of calling it from the setter in
   * model as we do for other fields that need to fire validation checks.
   * <p/>
   * TODO: There could be a better way?
   *
   * @param elements
   */
  public void setPreviewState( List<MongoDocumentField> elements ) {
    validate();
  }

  /**
   * In order for the model to control validation, the external container needs to register
   * the model instead of the XUL dialog... but don't prevent the dialog from registering with
   * listeners as well.
   */
  @Override public void addPropertyChangeListener( PropertyChangeListener listener ) {
    model.addPropertyChangeListener( listener );
    super.addPropertyChangeListener( listener );
  }

  @Override public boolean validate() {
    return model.validate();
  }

  protected void initializeXul() throws XulException {
    initializeXul( new SwingXulLoader(), new SwingBindingFactory(), new SwingXulRunner(), parent );
  }

  @Override public void onAccept() {
    model.save();
    close();
    dispose();

  }

  @Override public void onCancel() {
    stepname = null;
    close();
    dispose();

  }

  @Override protected Class<?> getClassForMessages() {
    return this.getClass();
  }

  @Override public void dispose() {

  }

  @Override public void clear() {
    if ( model == null ) {
      return;
    }
    model.clear();

    try {
      databaseBinding.fireSourceChanged();
      collectionBinding.fireSourceChanged();
    } catch ( Exception e ) {
      this.logError( "Non-critical error clearing database and collection information.", e );
    }
  }

  /**
   * Method bound to newitembinding attribute on XUL tree with ID "fieldsTable"; this will
   * be invoked from the context menu item "Insert new row".
   */
  public void createNewField() {
    MongoDocumentField field = new MongoDocumentField();
    model.getFields().add( field );
  }

  public void getDatabaseNamesFromMongo() {
    try {
      Vector<String> dbs = model.getDatabaseNamesFromMongo();
      model.setDbNames( dbs );
      databaseBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch ( Exception e ) {
      showMessage( e.getMessage(), "MongoDb Error" );
    }
  }

  public void getCollectionNamesFromMongo() {
    try {
      Vector<String> collections = model.getCollectionNamesFromMongo();
      model.setCollections( collections );
      collectionBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch ( Exception e ) {
      showMessage( e.getMessage(), "MongoDb Error" );
    }
  }

  /**
   * This method is invoked from the XUL definition; bound to the "fields" button on the Fields tab.
   */
  public void getDocumentFieldsFromMongo() {

    if ( Const.isEmpty( model.getHostnames() ) ) {
      showMessage( "At least one host name is required. Return to the configure tab, enter a host name and try again.",
          "MongoDb Error" );
      return;
    }

    if ( Const.isEmpty( model.getDbName() ) ) {
      showMessage( "A database name is required. Return to the options tab, enter a database name and try again.",
          "MongoDb Error" );
      return;
    }

    if ( Const.isEmpty( model.getCollection() ) ) {
      showMessage( "A collection name is required. Return to the options tab, enter a collection name and try again.",
          "MongoDb Error" );
      return;
    }

    try {

      // 0 = Add new
      // 1 = Add all
      // 2 = Clear and add all 
      // 3 = Cancel

      int mergeStrategy = 1;

      if ( model.getFields().size() > 0 ) {
        mergeStrategy = this.showClearDataMessage();
      }

      if ( ( mergeStrategy < 3 ) && ( mergeStrategy > -1 ) ) {
        model.getFieldsFromMongo( mergeStrategy );
      }

    } catch ( Exception e ) {
      showMessage( e.getMessage(), "MongoDb Error" );
    }
  }

  private static class IsEmptyStringToBooleanConvertor extends BindingConvertor<String, Boolean> {

    @Override public Boolean sourceToTarget( String value ) {
      return Const.isEmpty( value );

    }

    @Override public String targetToSource( Boolean value ) {
      return null;
    }
  }

  /**
   * Convertor logic for enabling/disabling the "Join Tags" button.
   * Disable if the Tags table has one or less selections; or the
   * inverse, enable if the table has 2 or more selections.
   */
  private static class SelectedToBooleanConvertor extends BindingConvertor<int[], Boolean> {

    @Override public Boolean sourceToTarget( int[] value ) {
      return value.length <= 1;

    }

    @Override public int[] targetToSource( Boolean value ) {
      return null;
    }
  }

  /**
   * Convertor logic for enabling/disabling the "Test Tags" button.
   * Disable if the Tags table has zero rows; or the
   * inverse, enable if the table has 1 or more rows.
   *
   * @param <T>
   */
  private static class RowCountToBooleanConvertor<T> extends BindingConvertor<Collection<T>, Boolean> {

    @Override public Boolean sourceToTarget( Collection<T> value ) {

      return ( value == null ) || ( value.isEmpty() );

    }

    @Override public Collection<T> targetToSource( Boolean value ) {
      return null;
    }
  }

  /**
   *
   */
  public void getTagsFromMongo() {
    try {

      if ( model.getReadPreference().equalsIgnoreCase( NamedReadPreference.PRIMARY.getName() ) ) {
        showMessage( "Tag sets defined with a read preference of 'primary' will not be honored by MongoDb. \n"
            + "Consider changing your read preference to one other than primary.", "MongoDb Warning" );
      }

      // 0 = Add new
      // 1 = Add all
      // 2 = Clear and add all 
      // 3 = Cancel

      int mergeStrategy = 1;

      if ( model.getTags().size() > 0 ) {
        mergeStrategy = this.showClearDataMessage();
      }

      if ( ( mergeStrategy < 3 ) && ( mergeStrategy > -1 ) ) {
        model.getTagsFromMongo( mergeStrategy );
      }

    } catch ( Exception e ) {
      showMessage( e.getMessage(), "MongoDb Error" );
    }

  }

  /**
   *
   */
  public void joinTags() {

    XulTree table = (XulTree) document.getElementById( "tagsTable" );

    int[] selectedTags = table.getAbsoluteSelectedRows();
    Object[][] values = table.getValues();
    String concatenated = "";

    for ( int i : selectedTags ) {
      String tag = (String) values[i][0];
      concatenated =
          concatenated + ( ( concatenated.length() > 0 ) ? ( ( !concatenated.endsWith( "," ) ) ? ", " : "" ) : "" )
              + tag;
    }
    MongoTag tag = new MongoTag( concatenated );
    model.getTags().add( tag );

  }

  /**
   *
   */
  public void testTags() {
    try {
      List<String> results = model.testSelectedTags();

      if ( results == null ) {
        showMessage( "No matches found.", "Replica Set Member Matches" );
        return;
      }

      StringBuffer message = new StringBuffer();
      for ( String result : results ) {
        message.append( result ).append( "\n" );
      }

      showMessage( message.toString(), "Replica Set Member Matches" );

    } catch ( MongoDbException e ) {

      showMessage( e.getMessage(), "MongoDb Error" );

    }
  }

  /**
   *
   */
  public void createNewTag() {
    model.getTags().add( new MongoTag() );
  }

}
