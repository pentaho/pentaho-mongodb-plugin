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

package org.pentaho.reporting.ui.datasources.mongodb;

import org.junit.Test;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputXulDialog;

public class MongoDbDatasourceDialogTest {

  @Test public void dummyTest() {
    // Have to have at least one test method to run, otherwise JUnit packs a sad.
    // Main method contains the tests.
  }

  public static void main( String[] args ) {

    KettleLogStore.init();

    MongoDbInputMeta meta = new MongoDbInputMeta();
    meta.setJsonQuery( "{CITY:\"NYC\"}" );
    meta.setAuthenticationPassword( "password" );
    meta.setAuthenticationUser( "gmoran" );
    meta.setCollection( "big" );
    meta.setDbName( "data" );
    meta.setHostnames( "" );
    meta.setPort( "27017" );
    meta.setReadPreference( "Secondary preferred" );
    meta.setFieldsName( "{id:true}" );
    meta.setQueryIsPipeline( true );

    TransMeta trans = new TransMeta();
    StepMeta stepMeta = new StepMeta( "mongo_source", meta );
    trans.addStep( stepMeta );

    MongoDbInputXulDialog dlg = new MongoDbInputXulDialog( null, meta, trans, "mongo_source" );

    if ( dlg.open() != null ) {
      System.out.println( "Host name(s): ".concat( meta.getHostnames() != null ? meta.getHostnames() : "" ) );
      System.out.println( "Port: ".concat( meta.getPort() != null ? meta.getPort() : "" ) );
      System.out.println( "Database: ".concat( meta.getDbName() != null ? meta.getDbName() : "" ) );
      System.out.println( "Collection: ".concat( meta.getCollection() != null ? meta.getCollection() : "" ) );
      System.out.println( "User: ".concat( meta.getAuthenticationUser() != null ? meta.getAuthenticationUser() : "" ) );
      System.out.println(
          "Password: ".concat( meta.getAuthenticationPassword() != null ? meta.getAuthenticationPassword() : "" ) );
      System.out
          .println( "Connection Timeout: ".concat( meta.getConnectTimeout() != null ? meta.getConnectTimeout() : "" ) );
      System.out.println( "Socket Timeout: ".concat( meta.getSocketTimeout() != null ? meta.getSocketTimeout() : "" ) );
      System.out
          .println( "Read Preference: ".concat( meta.getReadPreference() != null ? meta.getReadPreference() : "" ) );
      System.out.println( "JSON Query: ".concat( meta.getJsonQuery() != null ? meta.getJsonQuery() : "" ) );
      System.out
          .println( "Is Agg Pipeline:".concat( meta.getQueryIsPipeline() ? "IS pipeline" : "IS NOT a pipeline" ) );
      System.out.println( "Field Expression: ".concat( meta.getFieldsName() != null ? meta.getFieldsName() : "" ) );
    }

  }

}
