/**
 * 
 */
package org.pentaho.di.ui.trans.steps.mongodbinput;

import java.beans.PropertyChangeListener;
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
 *
 */
public class MongoDbInputXulDialog extends BaseStepGenericXulDialog {
  
  private MongoDbModel model;
  
  private Binding databaseBinding;
  private Binding collectionBinding;
  
  protected static BindingConvertor<String, Boolean> emptyStringBinding = new IsEmptyStringToBooleanConvertor();

  public MongoDbInputXulDialog(Object parent, BaseStepMeta baseStepMeta, TransMeta transMeta, String stepname ) {
    super("org/pentaho/di/ui/trans/steps/mongodbinput/xul/mongodb_input.xul", parent, baseStepMeta, transMeta, stepname);
  }
  
  public XulSettingsManager getSettingsManager() {
    return null;
  }

  public ResourceBundle getResourceBundle() {
    return null;
  }
  
  public void init(){

      model = new MongoDbModel((MongoDbInputMeta)stepMeta.getStepMetaInterface());
    
    try {
      bf.setBindingType(Binding.Type.ONE_WAY);
      
      bf.createBinding( model, "possibleReadPreferences", "readPreference", "elements").fireSourceChanged();

      bf.createBinding( "hostName", "value", "getDbs", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getTags", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "joinTags", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "testTags", "disabled", emptyStringBinding ).fireSourceChanged();
      bf.createBinding( "hostName", "value", "getFields", "disabled", emptyStringBinding ).fireSourceChanged();
//      bf.createBinding( "database", "value", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
 //     bf.createBinding( "database", "selectedItem", "getCollections", "disabled", emptyStringBinding ).fireSourceChanged();
           
      databaseBinding = bf.createBinding( model, "dbNames", "database", "elements");
      databaseBinding.fireSourceChanged();
      
      collectionBinding = bf.createBinding(model, "collections", "collection", "elements");
      collectionBinding.fireSourceChanged();
      
      // this controls enabling the external preview function ...
      bf.createBinding( model.getFields(), "children", this, "previewState").fireSourceChanged();

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
      bf.createBinding( model, "useAllReplicaMembers", "isUseAllReplicas", "checked").fireSourceChanged();

      bf.createBinding( model, "dbName", "database", "selectedItem").fireSourceChanged();
      bf.createBinding( model, "collection", "collection", "selectedItem").fireSourceChanged();
      bf.createBinding( model, "readPreference", "readPreference", "selectedItem").fireSourceChanged();

      bf.createBinding("isAggPipeline", "checked", "fieldsQuery", "disabled").fireSourceChanged();

      Binding fieldsBinding = bf.createBinding(model.getFields(), "children", "fieldsTable", "elements");
      fieldsBinding.fireSourceChanged();
      
      Binding tagsBinding = bf.createBinding(model.getTags(), "children", "tagsTable", "elements");
      tagsBinding.fireSourceChanged();

    } catch (Exception e) {
      log.logError("Error creating bindings for dialog. This dialog will not be available", e);

    }
  }
  
  /**
   * Because we are using an AbstractModelList as our fields container, we need 
   * to bind the validate() method here, instead of calling it from the setter in 
   * model as we do for other fields that need to fire validation checks. 
   * 
   * TODO: There could be a better way? 
   * 
   * @param elements
   */
  public void setPreviewState(List<MongoDocumentField> elements){
    validate();
  }
  
  /**
   * In order for the model to control validation, the external container needs to register 
   * the model instead of the XUL dialog... but don't prevent the dialog from registering with 
   * listeners as well. 
   */
  @Override
  public void addPropertyChangeListener(PropertyChangeListener listener) {
    model.addPropertyChangeListener(listener);
    super.addPropertyChangeListener(listener);
  }

  @Override
  public boolean validate(){
   return model.validate();
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
  
  @Override
  public void clear() {
    if (model == null)
    {
      return;
    }
    model.clear();
    
    try {
      databaseBinding.fireSourceChanged();
      collectionBinding.fireSourceChanged();
    } catch (Exception e) {
      this.logError("Non-critical error clearing database and collection information.", e);
    }
  }
  
  /**
   *  Method bound to newitembinding attribute on XUL tree with ID "fieldsTable"; this will  
   *  be invoked from the context menu item "Insert new row".
   */
  public void createNewField(){
    MongoDocumentField field = new MongoDocumentField();
    model.getFields().add(field);
  }

  public void getDatabaseNamesFromMongo(){
    try {
      Vector<String> dbs = model.getDatabaseNamesFromMongo();
      model.setDbNames(dbs);
      databaseBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch (Exception e) {
      showMessage(e.getMessage(), "MongoDb Error");
    }
  }

  public void getCollectionNamesFromMongo(){
    try {
      Vector<String> collections = model.getCollectionNamesFromMongo();
      model.setCollections(collections);
      collectionBinding.fireSourceChanged(); // should I be doing this, or is there a better way? 
    } catch (Exception e) {
      showMessage(e.getMessage(), "MongoDb Error");
    }
  }
  
  /**
   * This method is invoked from the XUL definition; bound to the "fields" button on the Fields tab. 
   */
  public void getDocumentFieldsFromMongo(){
    try {
      
      model.getFieldsFromMongo();

    } catch (Exception e) {
      showMessage(e.getMessage(), "MongoDb Error");
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
  
  /**
   * 
   */
  public void getTagsFromMongo(){
    try {
      
      if (model.getReadPreference() == NamedReadPreference.PRIMARY.getName()){
        showMessage("Tag sets defined with a read preference of Primary will not honored by MongoDb. \n" +  
                     "Consider changing your read preference to one other than Primary.", "MongoDb Warning");
      }
      
      model.getTagsFromMongo();

    } catch (Exception e) {
      showMessage(e.getMessage(), "MongoDb Error");
    }
    
  }
  
  /**
   * 
   */
  public void joinTags(){

    XulTree table = (XulTree)document.getElementById("tagsTable");
    
    int[] selectedTags = table.getAbsoluteSelectedRows();
    Object[][] values = table.getValues();
    String concatenated = "";
    
    for (int i : selectedTags) {
      String tag = (String) values[i][0];
      concatenated = concatenated
          + ((concatenated.length() > 0) ? ((!concatenated
              .endsWith(",")) ? ", " : "") : "") + tag; 
    }
    MongoTag tag = new MongoTag(concatenated);
    model.getTags().add(tag);
   
  }
  
  /**
   * 
   */
  public void testTags(){
    try {
      List<String> results = model.testSelectedTags();
      
      StringBuffer message = new StringBuffer();
      for (String result : results) {
        message.append(result).append("\n");
      }
      
      this.showMessage(message.toString(), "Replica Set Member Matches");
    
    } catch (MongoDbException e) {

      showMessage(e.getMessage(), "MongoDb Error");
    
    }
  }
  
  /**
   * 
   */
  public void createNewTag(){
    model.getTags().add(new MongoTag());
  }

    
}
