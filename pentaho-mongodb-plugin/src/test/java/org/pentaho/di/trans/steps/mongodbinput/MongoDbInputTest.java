/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.StrictStubs.class )
public class MongoDbInputTest extends BaseMongoDbStepTest {
  @Mock private MongoDbInputData stepDataInterface;
  @Mock private MongoDbInputMeta stepMetaInterface;
  @Mock private MongoCursorWrapper mockCursor;

  private Object[] putRow;
  private MongoDbInput dbInput;

  @Before public void before() throws MongoDbException {
    super.before();

    when( stepMetaInterface.getPort() ).thenReturn( "1010" );
    when( stepMetaInterface.getHostnames() ).thenReturn( "host" );

    dbInput = new MongoDbInput( stepMeta, stepDataInterface, 1, transMeta, trans ) {
      public Object[] getRow() {
        return rowData;
      }

      public RowMetaInterface getInputRowMeta() {
        return rowMeta;
      }

      public void putRow( RowMetaInterface rowMeta, Object[] row ) {
        putRow = row;
      }
    };
  }


  @Test public void testInitNoDbSpecified() {
    assertFalse( dbInput.init( stepMetaInterface, stepDataInterface ) );
    verify( mockLog ).logError( anyString(), throwableCaptor.capture() );
    assertThat( throwableCaptor.getValue().getMessage(), containsString( "No database specified" ) );
  }

  @Test public void testInitNoCollection() {
    when( stepMetaInterface.getDbName() ).thenReturn( "dbname" );
    assertFalse( dbInput.init( stepMetaInterface, stepDataInterface ) );
    verify( mockLog ).logError( anyString(), throwableCaptor.capture() );
    assertThat( throwableCaptor.getValue().getMessage(), containsString( "No collection specified" ) );
  }

  @Test public void testInit() throws MongoDbException {
    setupReturns();
    assertTrue( dbInput.init( stepMetaInterface, stepDataInterface ) );
    assertThat( stepDataInterface.clientWrapper, equalTo( mongoClientWrapper ) );
    assertThat( stepDataInterface.collection, equalTo( mongoCollectionWrapper ) );
  }

  private void setupReturns() throws MongoDbException {
    when( stepMetaInterface.getDbName() ).thenReturn( "dbname" );
    when( stepMetaInterface.getCollection() ).thenReturn( "collection" );
    when( mongoClientWrapper.getCollection( "dbname", "collection" ) ).thenReturn( mongoCollectionWrapper );
    when( mongoClientWrapper.getCollection( "dbname", "collection" ) ).thenReturn( mongoCollectionWrapper );
    when( mongoCollectionWrapper.find() ).thenReturn( mockCursor );
    when( mongoCollectionWrapper.find( Mockito.<DBObject>any(), Mockito.<DBObject>any() ) ).thenReturn( mockCursor );
  }

  @Test public void processRowSinglePartAggPipelineQuery() throws KettleException, MongoDbException {
    processRowWithQuery( "{$match : { foo : 'bar'}}" );
  }

  @Test public void processRowMultipartAggPipelineQuery() throws MongoDbException, KettleException {
    processRowWithQuery( "{$match : { foo : 'bar'}}, { $sort : 1 }" );
  }

  private void processRowWithQuery( String query ) throws MongoDbException, KettleException {
    setupReturns();
    when( stepMetaInterface.getJsonQuery() ).thenReturn( query );
    when( stepMetaInterface.getQueryIsPipeline() ).thenReturn( true );
    when( stepMetaInterface.isAllowDiskUse() ).thenReturn( false );
    String[] parts = query.split( "," );
    DBObject dbObjQuery = (DBObject) JSON.parse( parts[ 0 ] );
    DBObject[] remainder = parts.length > 1
      ? new DBObject[] { (DBObject) JSON.parse( parts[ 1 ] ) }
      : new DBObject[ 0 ];
    when( mongoCollectionWrapper.aggregate( dbObjQuery, remainder, stepMetaInterface.isAllowDiskUse() ) ).thenReturn( cursor );

    dbInput.init( stepMetaInterface, stepDataInterface );
    dbInput.processRow( stepMetaInterface, stepDataInterface );

    verify( stepDataInterface ).init();
    verify( mongoCollectionWrapper ).aggregate( dbObjQuery, remainder, stepMetaInterface.isAllowDiskUse() );
    assertEquals( cursor, stepDataInterface.m_pipelineResult );
  }

  @Test public void processRowMultipartAggPipelineQueryWithAllowDiskUse() throws MongoDbException, KettleException {
    processRowWithQueryAllowDiskUse( "{$match : { foo : 'bar'}}, { $sort : 1 }" );
  }

  private void processRowWithQueryAllowDiskUse( String query ) throws MongoDbException, KettleException {
    setupReturns();
    when( stepMetaInterface.getJsonQuery() ).thenReturn( query );
    when( stepMetaInterface.getQueryIsPipeline() ).thenReturn( true );
    when( stepMetaInterface.isAllowDiskUse() ).thenReturn( true );
    String[] parts = query.split( "," );
    DBObject dbObjQuery = (DBObject) JSON.parse( parts[ 0 ] );
    DBObject[] remainder = parts.length > 1
      ? new DBObject[] { (DBObject) JSON.parse( parts[ 1 ] ) }
      : new DBObject[ 0 ];
    when( mongoCollectionWrapper.aggregate( dbObjQuery, remainder, stepMetaInterface.isAllowDiskUse() ) ).thenReturn( cursor );

    dbInput.init( stepMetaInterface, stepDataInterface );
    dbInput.processRow( stepMetaInterface, stepDataInterface );

    verify( stepDataInterface ).init();
    verify( mongoCollectionWrapper ).aggregate( dbObjQuery, remainder, stepMetaInterface.isAllowDiskUse() );
    assertEquals( cursor, stepDataInterface.m_pipelineResult );
  }

  @Test public void testSimpleFind() throws KettleException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    dbInput.init( stepMetaInterface, stepDataInterface );
    assertFalse( "should return false as there are no more results",
      dbInput.processRow( stepMetaInterface, stepDataInterface ) );
    verify( mongoCollectionWrapper ).find();
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );
  }

  @Test public void testDispose() throws KettleException, MongoDbException {
    setupReturns();
    dbInput.init( stepMetaInterface, stepDataInterface );
    dbInput.processRow( stepMetaInterface, stepDataInterface );
    dbInput.dispose( stepMetaInterface, stepDataInterface );
    verify( mockCursor ).close();
    verify( mongoClientWrapper ).dispose();

    MongoDbException mockException = mock( MongoDbException.class );
    when( mockException.getMessage() ).thenReturn( "error msg" );
    doThrow( mockException ).when( mockCursor ).close();
    doThrow( mockException ).when( mongoClientWrapper ).dispose();
    dbInput.dispose( stepMetaInterface, stepDataInterface );
    // error should be logged after curor.close failure and client.dispose failure.
    verify( mockLog, times( 2 ) ).logError( "error msg" );
  }

  @Test public void testFindWithMoreResults() throws KettleException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    when( mockCursor.hasNext() ).thenReturn( true );
    ServerAddress serverAddress = mock( ServerAddress.class );
    when( serverAddress.toString() ).thenReturn( "serveraddress" );
    when( mockCursor.getServerAddress() ).thenReturn( serverAddress );
    DBObject nextDoc = (DBObject) JSON.parse( "{ 'foo' : 'bar' }" );
    when( mockCursor.next() ).thenReturn( nextDoc );
    dbInput.setStopped( false );
    dbInput.init( stepMetaInterface, stepDataInterface );

    assertTrue( "more results -> should return true", dbInput.processRow( stepMetaInterface, stepDataInterface ) );
    verify( mongoCollectionWrapper ).find();
    verify( mockCursor ).next();
    verify( mockLog ).logBasic( stringCaptor.capture() );
    assertThat( stringCaptor.getValue(), containsString( "serveraddress" ) );
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );
    assertThat( putRow[ 0 ], CoreMatchers.<Object>equalTo( JSON.serialize( nextDoc ) ) );
  }

  @Test public void testFindWithQuery() throws KettleException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    String query = "{ type : 'heavy'} ";
    when( stepMetaInterface.getJsonQuery() ).thenReturn( query );
    dbInput.init( stepMetaInterface, stepDataInterface );
    assertFalse( "should return false as there are no more results",
      dbInput.processRow( stepMetaInterface, stepDataInterface ) );
    verify( mongoCollectionWrapper ).find( dbObjectCaptor.capture(), Mockito.<DBObject>any() );
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );
    assertThat( dbObjectCaptor.getValue(), equalTo( (DBObject) JSON.parse( query ) ) );
  }

  @Test public void testAuthUserLogged() throws MongoDbException {
    setupReturns();
    when( stepMetaInterface.getAuthenticationUser() )
      .thenReturn( "joe_user" );

    dbInput.init( stepMetaInterface, stepDataInterface );
    verify( mockLog ).logBasic( stringCaptor.capture() );
    assertThat( stringCaptor.getValue(), containsString( "joe_user" ) );
  }

  @Test public void testExecuteForEachIncomingRow() throws MongoDbException, KettleException {
    setupReturns();
    when( stepMetaInterface.getExecuteForEachIncomingRow() )
      .thenReturn( true );
    when( stepMetaInterface.getJsonQuery() ).thenReturn( "{ foo : ?{param}} " );
    rowData = new Object[] { "'bar'" };
    rowMeta.addValueMeta( new ValueMetaString( "param" ) );
    dbInput.init( stepMetaInterface, stepDataInterface );
    assertTrue( dbInput.processRow( stepMetaInterface, stepDataInterface ) );
    verify( mongoCollectionWrapper ).find( dbObjectCaptor.capture(), Mockito.<DBObject>any() );
    assertThat( dbObjectCaptor.getValue(),
      equalTo( (DBObject) JSON.parse( "{foo : 'bar'}" ) ) );
  }

  @Test public void testOutputRowsForExecuteForEachIncomingRowTrue() throws MongoDbException, KettleException {
    setupReturns();
    when( stepMetaInterface.getExecuteForEachIncomingRow() )
      .thenReturn( true );
    when( stepMetaInterface.getOutputJson() )
      .thenReturn( true );
    when( stepMetaInterface.getJsonQuery() ).thenReturn( "{ Company : ?{input}}" );
    Object[] row = { "'HC'", "jeevan",
      "{ '_id' : 'ObjectId(60e433324a1cb8ec4ccd9758)','Company' : 'Portugal','Name': 'steve' ,'gender' : 'Male' }" };

    rowData = new Object[] { "'HC'", "jeevan" };
    when( stepMetaInterface.getOutputJson() ).thenReturn( true );
    ValueMetaInterface rowValueMeta1 = new ValueMetaString( "input" );
    ValueMetaInterface rowValueMeta2 = new ValueMeta( "input company", ValueMetaInterface.TYPE_STRING );

    rowMeta.addValueMeta( rowValueMeta1 );
    rowMeta.addValueMeta( rowValueMeta2 );

    when( mockCursor.hasNext() ).thenReturn( true );
    ServerAddress serverAddress = mock( ServerAddress.class );
    when( serverAddress.toString() ).thenReturn( "serveraddress" );
    when( mockCursor.getServerAddress() ).thenReturn( serverAddress );
    DBObject nextDoc = (DBObject) JSON.parse( "{ '_id' : 'ObjectId(60e433324a1cb8ec4ccd9758)',"
      + "'Company' : 'Portugal','Name': 'steve' ,'gender' : 'Male' }" );
    when( mockCursor.next() ).thenReturn( nextDoc );
    dbInput.setStopped( false );
    dbInput.init( stepMetaInterface, stepDataInterface );

    assertTrue( dbInput.processRow( stepMetaInterface, stepDataInterface ) );

    verify( mongoCollectionWrapper ).find( dbObjectCaptor.capture(), Mockito.<DBObject>any() );
    verify( mockCursor ).next();
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );


    assertThat( dbObjectCaptor.getValue(),
      equalTo( (DBObject) JSON.parse( "{Company : 'HC'}" ) ) );
    assertThat( putRow[ 0 ], equalTo( row[ 0 ] ) );
    assertThat( putRow[ 1 ], equalTo( row[ 1 ] ) );
    assertThat( putRow[ 2 ], CoreMatchers.<Object>equalTo( JSON.serialize( nextDoc ) ) );
  }

  @Test public void testOutputRowsForExecuteForEachIncomingRowFalse() throws MongoDbException, KettleException {
    setupReturns();
    when( stepMetaInterface.getExecuteForEachIncomingRow() )
      .thenReturn( false );
    when( stepMetaInterface.getOutputJson() )
      .thenReturn( true );
    Object[] row = { "HC", "jeevan",
      "{ '_id' : 'ObjectId(60e433324a1cb8ec4ccd9758)','Company' : 'Portugal','Name': 'steve' ,'gender' : 'Male' }" };

    rowData = new Object[] { "HC", "jeevan" };
    when( stepMetaInterface.getOutputJson() ).thenReturn( true );
    ValueMetaInterface rowValueMeta1 = new ValueMetaString( "input" );
    ValueMetaInterface rowValueMeta2 = new ValueMeta( "input company", ValueMetaInterface.TYPE_STRING );

    rowMeta.addValueMeta( rowValueMeta1 );
    rowMeta.addValueMeta( rowValueMeta2 );

    when( mockCursor.hasNext() ).thenReturn( true );
    ServerAddress serverAddress = mock( ServerAddress.class );
    when( serverAddress.toString() ).thenReturn( "serveraddress" );
    when( mockCursor.getServerAddress() ).thenReturn( serverAddress );
    DBObject nextDoc = (DBObject) JSON.parse(
      "{ '_id' : 'ObjectId(60e433324a1cb8ec4ccd9758)','Company' : 'Portugal','Name': 'steve' ,'gender' : 'Male' }" );
    when( mockCursor.next() ).thenReturn( nextDoc );
    dbInput.setStopped( false );
    dbInput.init( stepMetaInterface, stepDataInterface );

    assertTrue( dbInput.processRow( stepMetaInterface, stepDataInterface ) );

    verify( mongoCollectionWrapper ).find();
    verify( mockCursor ).next();
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );

    assertThat( putRow[ 0 ], CoreMatchers.<Object>equalTo( JSON.serialize( nextDoc ) ) );
  }

  @Test public void testOutputRowsForExecuteForEachIncomingRowFields() throws MongoDbException, KettleException  {
    setupReturns();
    String fieldName1 = "input";
    String fieldName2 = "input company";
    List<MongoField> lsm = new ArrayList<MongoField>();
    MongoField mongoField1 = new MongoField();
    mongoField1.m_fieldName = fieldName1;
    mongoField1.m_fieldPath = "$.name";
    mongoField1.m_kettleType = "String";
    MongoField mongoField2 = new MongoField();
    mongoField2.m_fieldName = fieldName2;
    mongoField2.m_fieldPath = "$.company";
    mongoField2.m_kettleType = "String";
    lsm.add( mongoField1 );
    lsm.add( mongoField2 );

    ValueMetaInterface rowValueMeta1 = new ValueMetaString( fieldName1 );
    ValueMetaInterface rowValueMeta2 = new ValueMeta( fieldName2, ValueMetaInterface.TYPE_STRING );

    rowMeta.addValueMeta( rowValueMeta1 );
    rowMeta.addValueMeta( rowValueMeta2 );

    when( stepMetaInterface.getExecuteForEachIncomingRow() ).thenReturn( true );
    when( stepMetaInterface.getOutputJson() ).thenReturn( false );
    when( stepMetaInterface.getMongoFields() ).thenReturn( lsm );

    when( mockCursor.hasNext() ).thenReturn( true );
    ServerAddress serverAddress = mock( ServerAddress.class );
    when( serverAddress.toString() ).thenReturn( "serveraddress" );
    when( mockCursor.getServerAddress() ).thenReturn( serverAddress );

    DBObject nextDoc = (DBObject) JSON.parse( "{ '_id' : 'ObjectId(60e433324a1cb8ec4ccd9758)', 'Company' : 'Portugal','Name': 'steve' ,'gender' : 'Male' }" );
    Object[][] output = new Object[][]{ { null, null, "HC", 1000 } };

    when( stepDataInterface.mongoDocumentToKettle( nextDoc, dbInput ) ).thenReturn( output );
    when( mockCursor.next() ).thenReturn( nextDoc );

    Answer<Void> mongoFieldsAnswer = new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocationOnMock ) throws Throwable {
        ValueMetaInterface mongoField1 = new ValueMetaString( "Company" );
        ValueMetaInterface mongoField2 = new ValueMeta( "sal", ValueMetaInterface.TYPE_NUMBER );
        RowMetaInterface rowMeta = (RowMetaInterface) invocationOnMock.getArguments()[ 0 ];
        rowMeta.addValueMeta( mongoField1 );
        rowMeta.addValueMeta( mongoField2 );
        return null;
      }
    };

    Mockito.lenient().doAnswer( mongoFieldsAnswer ).when( stepMetaInterface ).getFields( rowMeta, "MongoDB Input", null, stepMeta, dbInput );
    rowData = new Object[] { "HC", "john" };

    dbInput.setStopped( false );
    dbInput.init( stepMetaInterface, stepDataInterface );

    assertTrue( dbInput.processRow( stepMetaInterface, stepDataInterface ) );

    verify( mongoCollectionWrapper ).find();
    verify( mockCursor ).next();
    assertThat( stepDataInterface.cursor, equalTo( mockCursor ) );
    Object[] row = {"HC", "john", "HC", 1000};
    assertThat( putRow[ 0 ], equalTo( row[ 0 ] ) );
    assertThat( putRow[ 1 ], equalTo( row[ 1 ] ) );
    assertThat( putRow[ 2 ], equalTo( row[ 2 ] ) );
    assertThat( putRow[ 3 ], equalTo( row[ 3 ] ) );
  }
}
