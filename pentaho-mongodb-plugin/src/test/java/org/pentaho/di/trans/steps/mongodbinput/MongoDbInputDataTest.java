/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputDialog;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoDbInputDataTest {
  private MongoDbInputData mongoDbInputData;

  protected static String s_testData =
      "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
          + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
          + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 =
      "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
          + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } }, "
          + "{ \"rec1\" : { \"f1\" : \"sid\", \"f2\" : \"zaphod\" } } ] }, "
          + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";

  static {
    try {
      ValueMetaPluginType.getInstance().searchPlugins();
    } catch ( KettlePluginException ex ) {
      ex.printStackTrace();
    }
  }

  @Before public void setUp() {
    mongoDbInputData = new MongoDbInputData();
  }

  @Test public void testDiscoverFields() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
    final List<MongoField> mongoFields = new ArrayList<MongoField>();

    doAnswer( new Answer() {
        @Override public Object answer( InvocationOnMock invocationOnMock ) {
          ( (DiscoverFieldsCallback) invocationOnMock.getArguments()[9] ).notifyFields( mongoFields );
          return null;
        }
      } ).when( mongoDbInputDiscoverFields )
        .discoverFields( Mockito.<MongoProperties.Builder>any(), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), Mockito.<MongoDbInputMeta>any(), Mockito.<VariableSpace>any(), Mockito.<DiscoverFieldsCallback>any() );

    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    MongoDbInputDialog.discoverFields( meta, vars, docsToSample, dialog );
    verify( holder, atLeastOnce() ).getMongoDbInputDiscoverFields();

    //Test case when docsToSample is zero
    MongoDbInputDialog.discoverFields( meta, vars, 0, dialog );
    verify( holder, atLeastOnce() ).getMongoDbInputDiscoverFields();
  }

  @Test public void testDiscoverFieldsExceptionCallback() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );

    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocationOnMock ) {
        ( (DiscoverFieldsCallback) invocationOnMock.getArguments()[9] ).notifyException( new KettleException() );
        return null;
      }
    } ).when( mongoDbInputDiscoverFields )
      .discoverFields( Mockito.<MongoProperties.Builder>any(), any(), anyString(), anyString(), anyString(),
        anyBoolean(), anyInt(), Mockito.<MongoDbInputMeta>any(), Mockito.<VariableSpace>any(), Mockito.<DiscoverFieldsCallback>any() );

    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    MongoDbInputDialog.discoverFields( meta, vars, docsToSample, dialog );
    verify( dialog, atLeastOnce() ).handleNotificationException( any( Exception.class ) );
  }

  @Test public void testDiscoverFieldsThrowsException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    MongoDbInputDialog dialog = mock( MongoDbInputDialog.class );

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );

    doThrow( new KettleException() ).when( mongoDbInputDiscoverFields )
      .discoverFields( Mockito.<MongoProperties.Builder>any(), anyString(), anyString(), anyString(), anyString(),
        anyBoolean(), anyInt(), Mockito.<MongoDbInputMeta>any(), Mockito.<VariableSpace>any(), Mockito.<DiscoverFieldsCallback>any() );

    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    try {
      MongoDbInputDialog.discoverFields( meta, vars, docsToSample, dialog );
    } catch ( Exception expected ) {
      //expected
    }
  }

  @Test public void testDiscoverFieldsWithoutCallback() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
    List<MongoField> mongoFields = new ArrayList<MongoField>();
    mongoFields.add( new MongoField() );
    when( mongoDbInputDiscoverFields
        .discoverFields( Mockito.<MongoProperties.Builder>any(), any(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), Mockito.<MongoDbInputMeta>any(), Mockito.<VariableSpace>any() ) ).thenReturn( mongoFields );
    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    boolean result = MongoDbInputDialog.discoverFields( meta, vars, docsToSample );
    assertTrue( result );

    //Test case when docsToSample is zero
    result = MongoDbInputDialog.discoverFields( meta, vars, 0 );
    assertTrue( result );

    //Test case when no fields are found
    mongoFields.clear();
    result = MongoDbInputDialog.discoverFields( meta, vars, docsToSample );
    assertFalse( result );
  }

  @Test public void testDiscoverFieldsWithoutCallbackThrowsKettleException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
    when( mongoDbInputDiscoverFields
        .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ), Mockito.<VariableSpace>any() ) )
        .thenThrow( new KettleException( "testException" ) );
    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    try {
      MongoDbInputDialog.discoverFields( meta, vars, docsToSample );
    } catch ( KettleException e ) {
      //Expected
    }
  }

  @Test public void testDiscoverFieldsWithoutCallbackThrowsException() throws Exception {
    String dbName = "testDb";
    String collection = "testCollection";
    String query = "testQuery";
    String fields = "testFields";

    MongoDbInputMeta meta = mock( MongoDbInputMeta.class );
    when( meta.getName() ).thenReturn( dbName );
    when( meta.getCollection() ).thenReturn( collection );
    when( meta.getJsonQuery() ).thenReturn( query );
    when( meta.getFieldsName() ).thenReturn( fields );

    VariableSpace vars = mock( VariableSpace.class );
    when( vars.environmentSubstitute( dbName ) ).thenReturn( dbName );
    when( vars.environmentSubstitute( collection ) ).thenReturn( collection );
    when( vars.environmentSubstitute( query ) ).thenReturn( query );
    when( vars.environmentSubstitute( fields ) ).thenReturn( fields );

    int docsToSample = 1;

    //Mock the discoverFields call so that it returns a list of mongofields from the expected input
    MongoDbInputDiscoverFieldsHolder holder = mock( MongoDbInputDiscoverFieldsHolder.class );
    MongoDbInputDiscoverFields mongoDbInputDiscoverFields = mock( MongoDbInputDiscoverFields.class );
    when( mongoDbInputDiscoverFields
        .discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(), anyString(), anyString(),
            anyBoolean(), anyInt(), any( MongoDbInputMeta.class ), Mockito.<VariableSpace>any() ) ).thenThrow( new NullPointerException() );
    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    try {
      MongoDbInputDialog.discoverFields( meta, vars, docsToSample );
    } catch ( KettleException e ) {
      //Expected
    }
  }

  @Test public void testGetNonExistentField() throws KettleException {
    Object mongoO = JSON.parse( s_testData );
    assertTrue( mongoO instanceof DBObject );

    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.iDontExist";
    mm.m_kettleType = "String";
    discoveredFields.add( mm );

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
    assertEquals( 1, result.length - RowDataUtil.OVER_ALLOCATE_SIZE );
    assertTrue( result[0] == null );
  }

  @Test public void testArrayUnwindArrayFieldsOnly() throws KettleException {
    Object mongoO = JSON.parse( s_testData2 );
    assertTrue( mongoO instanceof DBObject );

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";

    fields.add( mm );
    RowMetaInterface rowMeta = new RowMeta();
    for ( MongoField m : fields ) {
      ValueMetaInterface vm = new ValueMeta( m.m_fieldName, ValueMeta.getType( m.m_kettleType ) );
      rowMeta.addValueMeta( vm );
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields( fields );
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle( (DBObject) mongoO, vars );

    assertTrue( result != null );
    assertEquals( 2, result.length );

    // should be two rows returned due to the array expansion
    assertTrue( result[0] != null );
    assertTrue( result[1] != null );
    assertEquals( "bob", result[0][0] );
    assertEquals( "sid", result[1][0] );
  }

  @Test public void testArrayUnwindOneArrayExpandFieldAndOneNormalField() throws KettleException {
    Object mongoO = JSON.parse( s_testData2 );
    assertTrue( mongoO instanceof DBObject );

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add( mm );

    mm = new MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.name";
    mm.m_kettleType = "String";
    fields.add( mm );

    RowMetaInterface rowMeta = new RowMeta();
    for ( MongoField m : fields ) {
      ValueMetaInterface vm = new ValueMeta( m.m_fieldName, ValueMeta.getType( m.m_kettleType ) );
      rowMeta.addValueMeta( vm );
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields( fields );
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle( (DBObject) mongoO, vars );

    assertTrue( result != null );
    assertEquals( 2, result.length );

    // each row should have two entries
    assertEquals( 2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length );

    // should be two rows returned due to the array expansion
    assertTrue( result[0] != null );
    assertTrue( result[1] != null );
    assertEquals( "bob", result[0][0] );
    assertEquals( "sid", result[1][0] );

    // george should be the name in both rows
    assertEquals( "george", result[0][1] );
    assertEquals( "george", result[1][1] );
  }

  @Test public void testArrayUnwindWithOneExistingAndOneNonExistingField() throws KettleException {
    Object mongoO = JSON.parse( s_testData2 );
    assertTrue( mongoO instanceof DBObject );

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add( mm );

    mm = new MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.one.two[*].rec6.nonExistent";
    mm.m_kettleType = "String";
    fields.add( mm );

    RowMetaInterface rowMeta = new RowMeta();
    for ( MongoField m : fields ) {
      ValueMetaInterface vm = new ValueMeta( m.m_fieldName, ValueMeta.getType( m.m_kettleType ) );
      rowMeta.addValueMeta( vm );
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields( fields );
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle( (DBObject) mongoO, vars );

    assertTrue( result != null );
    assertEquals( 2, result.length );

    // should be two rows returned due to the array expansion
    assertTrue( result[0] != null );
    assertTrue( result[1] != null );
    assertEquals( "bob", result[0][0] );
    assertEquals( "sid", result[1][0] );

    // each row should have two entries
    assertEquals( 2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length );

    // this field doesn't exist in the doc structure, so we expect null
    assertTrue( result[0][1] == null );
    assertTrue( result[1][1] == null );
  }

  @Test public void testCleansePath() {
    // param at end of path
    assertThat( MongoDbInputData.cleansePath( "my.path.with.${a.dot.param}" ),
        equalTo( "my.path.with.${a_dot_param}" ) );
    // param at start of path
    assertThat( MongoDbInputData.cleansePath( "${a.dot.param}.my.path.with" ),
        equalTo( "${a_dot_param}.my.path.with" ) );
    // param in middle of path
    assertThat( MongoDbInputData.cleansePath( "my.path.with.${a.dot.param}.otherstuff" ),
        equalTo( "my.path.with.${a_dot_param}.otherstuff" ) );
    // multiple params
    assertThat( MongoDbInputData.cleansePath( "my.${oneparam}.with.${a.dot.param}.otherstuff" ),
        equalTo( "my.${oneparam}.with.${a_dot_param}.otherstuff" ) );
  }

}
