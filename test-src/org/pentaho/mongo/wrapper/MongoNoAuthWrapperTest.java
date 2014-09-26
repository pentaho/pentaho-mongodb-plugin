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
package org.pentaho.mongo.wrapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.mongo.wrapper.field.MongoField;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
public class MongoNoAuthWrapperTest {
  protected static String s_testData = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";
  protected static String s_testData3 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : 42, anotherNumber : 32, numberArray : [1,2,3] }";
  protected static String s_testData4 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : \"Forty two\", anotherNumber : null, numberArray : [null,2,3] }";
  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, " + "\"members\" : [" + "{"
      + "\"_id\" : 0, " + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {" + "\"dc.one\" : \"primary\", "
      + "\"use\" : \"production\"" + "}" + "}, " + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
      + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{" + "\"_id\" : 2, "
      + "\"host\" : \"palladium.local:27019\", " + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
      + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {" + "\"getLastErrorModes\" : { "
      + "\"DCThree\" : {" + "\"dc.three\" : 1" + "}" + "}" + "}" + "}";
  public static String TAG_SET = "{\"use\" : \"production\"}";
  static {
    try {
      ValueMetaPluginType.getInstance().searchPlugins();
    } catch ( KettlePluginException ex ) {
      ex.printStackTrace();
    }
  }
  @Test
  public void testExtractLastErrorMode() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    assertTrue( config != null );
    List<String> lastErrorModes = new ArrayList<String>();
    new NoAuthMongoClientWrapper( null, null ).extractLastErrorModes( config, lastErrorModes );
    assertTrue( lastErrorModes.size() == 1 );
    assertEquals( "DCThree", lastErrorModes.get( 0 ) );
  }
  @Test
  public void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );
    assertTrue( members != null );
    assertTrue( members instanceof BasicDBList );
    assertEquals( 3, ( (BasicDBList) members ).size() );
  }
  @Test
  public void testGetAllTags() {
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );
    List<String> allTags = new NoAuthMongoClientWrapper( null, null ).setupAllTags( (BasicDBList) members );
    assertEquals( 4, allTags.size() );
  }
  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() {
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy
    DBObject tSet = (DBObject) JSON.parse( TAG_SET );
    tagSets.add( tSet );
    DBObject config = (DBObject) JSON.parse( REP_SET_CONFIG );
    Object members = config.get( NoAuthMongoClientWrapper.REPL_SET_MEMBERS );
    List<DBObject> satisfy =
        new NoAuthMongoClientWrapper( null, null ).checkForReplicaSetMembersThatSatisfyTagSets( tagSets,
            (BasicDBList) members );
    // two replica set members have the "use : production" tag in their tag sets
    assertEquals( 2, satisfy.size() );
  }
  @Test
  public void testDeterminePaths() {
    Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    Object mongoO = JSON.parse( s_testData );
    assertTrue( mongoO instanceof DBObject );
    NoAuthMongoClientWrapper.docToFields( (DBObject) mongoO, fieldLookup );
    NoAuthMongoClientWrapper.postProcessPaths( fieldLookup, discoveredFields, 1 );
    assertEquals( 5, discoveredFields.size() );
    // check types
    int stringCount = 0;
    int numCount = 0;
    for ( MongoField m : discoveredFields ) {
      if ( ValueMeta.getType( m.m_kettleType ) == ValueMetaInterface.TYPE_STRING ) {
        stringCount++;
      }
      if ( ValueMeta.getType( m.m_kettleType ) == ValueMetaInterface.TYPE_INTEGER ) {
        numCount++;
      }
    }
    assertEquals( 1, numCount );
    assertEquals( 4, stringCount );
  }
  @Test
  public void testDeterminePathsWithDisparateTypes() {
    Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    Object mongoO = JSON.parse( s_testData3 );
    assertTrue( mongoO instanceof DBObject );
    NoAuthMongoClientWrapper.docToFields( (DBObject) mongoO, fieldLookup );
    mongoO = JSON.parse( s_testData4 );
    assertTrue( mongoO instanceof DBObject );
    NoAuthMongoClientWrapper.docToFields( (DBObject) mongoO, fieldLookup );
    NoAuthMongoClientWrapper.postProcessPaths( fieldLookup, discoveredFields, 1 );
    assertEquals( 9, discoveredFields.size() );
    Collections.sort( discoveredFields );
    // First path is the "aNumber" field
    assertTrue( discoveredFields.get( 0 ).m_disparateTypes );
  }
  @Test
  public void testGetAllFields() throws KettleException {
    Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    Object mongoO = JSON.parse( s_testData );
    assertTrue( mongoO instanceof DBObject );
    NoAuthMongoClientWrapper.docToFields( (DBObject) mongoO, fieldLookup );
    NoAuthMongoClientWrapper.postProcessPaths( fieldLookup, discoveredFields, 1 );
    Collections.sort( discoveredFields );
    RowMetaInterface rowMeta = new RowMeta();
    for ( MongoField m : discoveredFields ) {
      ValueMetaInterface vm = new ValueMeta( m.m_fieldName, ValueMeta.getType( m.m_kettleType ) );
      rowMeta.addValueMeta( vm );
    }
    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields( discoveredFields );
    data.init();
    Variables vars = new Variables();
    Object[] result = data.mongoDocumentToKettle( (DBObject) mongoO, vars )[0];
    assertTrue( result != null );
    Object[] expected = { new Long( 42 ), "zzz", "bob", "fred", "george" };
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      assertTrue( result[i] != null );
      assertEquals( expected[i], result[i] );
    }
  }
}
