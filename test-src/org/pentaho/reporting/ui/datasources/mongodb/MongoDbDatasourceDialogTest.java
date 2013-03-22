package org.pentaho.reporting.ui.datasources.mongodb;

import org.junit.Test;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputXulDialog;

public class MongoDbDatasourceDialogTest {
  
  @Test
  public void dummyTest() {
    // Have to have at least one test method to run, otherwise JUnit packs a sad.
    // Main method contains the tests.
  }

  public static void main(String[] args) {

    MongoDbInputMeta meta = new MongoDbInputMeta();
    meta.setJsonQuery("{CITY:\"NYC\"}");
    meta.setAuthenticationPassword("password");
    meta.setAuthenticationUser("gmoran");
    meta.setCollection("big");
    meta.setDbName("data");
    meta.setHostnames("");
    meta.setPort("27017");
    meta.setReadPreference("Secondary preferred");
    meta.setFieldsName("{id:true}");
    meta.setQueryIsPipeline(true);
    
    MongoDbInputXulDialog dlg = new MongoDbInputXulDialog(null, meta, new TransMeta(), "mongo_source");
    
    if (dlg.open()!= null){
      System.out.println("Host name(s): ".concat(meta.getHostnames()!=null?meta.getHostnames():""));
      System.out.println("Port: ".concat(meta.getPort()!=null?meta.getPort():""));
      System.out.println("Database: ".concat(meta.getDbName()!=null?meta.getDbName():""));
      System.out.println("Collection: ".concat(meta.getCollection()!=null?meta.getCollection():""));
      System.out.println("User: ".concat(meta.getAuthenticationUser()!=null?meta.getAuthenticationUser():""));
      System.out.println("Password: ".concat(meta.getAuthenticationPassword()!=null?meta.getAuthenticationPassword():""));
      System.out.println("Connection Timeout: ".concat(meta.getConnectTimeout()!=null?meta.getConnectTimeout():""));
      System.out.println("Socket Timeout: ".concat(meta.getSocketTimeout()!=null?meta.getSocketTimeout():""));
      System.out.println("Read Preference: ".concat(meta.getReadPreference()!=null?meta.getReadPreference():""));
      System.out.println("JSON Query: ".concat(meta.getJsonQuery()!=null?meta.getJsonQuery():""));
      System.out.println("Is Agg Pipeline:".concat(meta.getQueryIsPipeline()?"IS pipeline":"IS NOT a pipeline"));
      System.out.println("Field Expression: ".concat(meta.getFieldsName()!=null?meta.getFieldsName():""));
    }

  }

}
