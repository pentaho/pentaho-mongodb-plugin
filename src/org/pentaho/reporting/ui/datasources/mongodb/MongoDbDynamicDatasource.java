package org.pentaho.reporting.ui.datasources.mongodb;

import org.pentaho.di.core.annotations.DataFactoryPlugin;
import org.pentaho.di.datafactory.DynamicDatasource;

@DataFactoryPlugin(id="MongoDB", name="MongoDb Datafactory")
public class MongoDbDynamicDatasource implements DynamicDatasource {

  public MongoDbDynamicDatasource() {
  }

  @Override
  public String getDialogClass() {
    return "org.pentaho.reporting.ui.datasources.mongodb.MongoDbDatasourceDialog";
  }

  @Override
  public String getTemplate() {
    return "template.ktr";
  }

  @Override
  public String getResourceEntryName() {
   
    return "mongo_transform";
  }

  @Override
  public String getStepName() {
    
    return "mongo_source_data";
    
  }

  @Override
  public String getQueryName() {
    return "Mongo Query";
  }

}
