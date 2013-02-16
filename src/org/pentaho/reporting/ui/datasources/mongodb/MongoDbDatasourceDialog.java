/**
 * 
 */
package org.pentaho.reporting.ui.datasources.mongodb;

import java.lang.reflect.InvocationTargetException;
import java.util.ResourceBundle;
import java.util.Vector;

import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.step.BaseStepGenericXulDialog;
import org.pentaho.reporting.ui.datasources.mongodb.models.MongoDbModel;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulSettingsManager;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.swing.SwingBindingFactory;
import org.pentaho.ui.xul.swing.SwingXulLoader;
import org.pentaho.ui.xul.swing.SwingXulRunner;

/**
 * @author gmoran
 * @date 01/28/2013
 *
 */
public class MongoDbDatasourceDialog extends BaseStepGenericXulDialog {
  
  private MongoDbModel model;
  private Binding databaseBinding;
  private Binding collectionBinding;
  private Binding fieldsBinding;
  
  protected static BindingConvertor<String, Boolean> emptyStringBinding = new IsEmptyStringToBooleanConvertor();

  public MongoDbDatasourceDialog(Object parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname ) {

    super("org/pentaho/reporting/ui/datasources/mongodb/xul/mongodb_input.xul", parent, baseStepMeta, transMeta, stepname);
    
    try {
      initializeXul();
    } catch (Exception e) {
      log.logError("Error initializing ("+stepname+") step dialog", e);
      throw new IllegalStateException("Cannot load dialog due to error in initialization", e);
    }
  }

  public XulSettingsManager getSettingsManager() {
    return null;
  }

  public ResourceBundle getResourceBundle() {
    return null;
  }
  
  public void init(){

      model = new MongoDbModel((MongoDbInputMeta)baseStepMeta);
    
    try {
      bf.setBindingType(Binding.Type.ONE_WAY);
      
      bf.createBinding( model, "possibleReadPreferences", "readPreference", "elements").fireSourceChanged();

      bf.createBinding( "hostName", "value", "getDbs", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
//      bf.createBinding( "database", "value", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
 //     bf.createBinding( "database", "selectedItem", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
           
      databaseBinding = bf.createBinding( model, "dbNames", "database", "elements");
      databaseBinding.fireSourceChanged();
      
      collectionBinding = bf.createBinding(model, "collections", "collection", "elements");
      collectionBinding.fireSourceChanged();
      
      bf.createBinding( this, "fieldValuesChanged", "fieldsTable", "onedit");

      bf.setBindingType(Binding.Type.BI_DIRECTIONAL);

      bf.createBinding( model, "hostnames", "hostName", "value").fireSourceChanged();
      bf.createBinding( model, "port", "port", "value").fireSourceChanged();
      bf.createBinding( model, "jsonQuery", "jsonQuery", "value").fireSourceChanged();
      bf.createBinding( model, "authenticationPassword", "authPassword", "value").fireSourceChanged();
      bf.createBinding( model, "authenticationUser", "authUser", "value").fireSourceChanged();
      bf.createBinding( model, "fieldsName", "fieldsQuery", "value").fireSourceChanged();
      bf.createBinding( model, "socketTimeout", "socketTimeout", "value").fireSourceChanged();
      bf.createBinding( model, "connectTimeout", "connectionTimeout", "value").fireSourceChanged();

      bf.createBinding( model, "queryIsPipeline", "isAggPipeline", "checked").fireSourceChanged();

      bf.createBinding( model, "dbName", "database", "selectedItem").fireSourceChanged();
      bf.createBinding( model, "collection", "collection", "selectedItem").fireSourceChanged();
      bf.createBinding( model, "readPreference", "readPreference", "selectedItem").fireSourceChanged();

      bf.createBinding("isAggPipeline", "checked", "fieldsQuery", "disabled").fireSourceChanged();
      
      fieldsBinding = bf.createBinding(model, "fields", "fieldsTable", "elements");
      fieldsBinding.fireSourceChanged();

    } catch (Exception e) {
      log.logError("Error creating bindings for dialog. This dialog will not be available", e);

    }
  }
  
  public String getFieldValuesChanged(){
    try {
      if (fieldsBinding != null){
        
        fieldsBinding.fireSourceChanged();
      }
    } catch (Exception e) {

      log.logError("Error updating fields.", e);

    }
    return null;
  }

  protected void initializeXul() throws XulException {

    initializeXul(new SwingXulLoader(), new SwingBindingFactory(), new SwingXulRunner(), parent);
      
  }

  @Override
  public void onAccept() {
    model.save();
    close();
    dispose();

  }

  @Override
  public void onCancel() {
    stepname = null;
    close();
    dispose();

  }

  @Override
  protected Class<?> getClassForMessages() {
    return this.getClass();
  }

  @Override
  public void dispose() {

  }
  
  public void getDatabaseNamesFromMongo(){
    try {
      Vector<String> dbs = model.getDatabaseNamesFromMongo();
      model.setDbNames(dbs);
      databaseBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch (Exception e) {
      this.logError("Error retrieving database names from MongoDB. Please check your connection information and try again.", e);
    }
  }

  public void getCollectionNamesFromMongo(){
    try {
      Vector<String> collections = model.getCollectionNamesFromMongo();
      model.setCollections(collections);
      collectionBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch (Exception e) {
      this.logError("Error retrieving collection names from MongoDB. Please check your connection information and database name and try again.", e);
    }
  }
  
  public void getDocumentFieldsFromMongo(){
    try {
      model.getFieldsFromMongo();
      fieldsBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch (Exception e) {
      this.logError("Error retrieving fields from MongoDB. Please check your connection information and database name and try again.", e);
    }
  }
  private static class IsEmptyStringToBooleanConvertor extends BindingConvertor<String, Boolean> {

    @Override
    public Boolean sourceToTarget(String value) {
      return Const.isEmpty(value);
      
    }

    @Override
    public String targetToSource(Boolean value) {
      return null;
    }
  }

}
