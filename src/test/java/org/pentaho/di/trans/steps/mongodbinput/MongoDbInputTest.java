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

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import java.util.Iterator;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

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
    when( mongoCollectionWrapper.find( any( DBObject.class ), any( DBObject.class ) ) ).thenReturn( mockCursor );
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
    String[] parts = query.split( "," );
    DBObject dbObjQuery = (DBObject) JSON.parse( parts[0] );
    DBObject[] remainder = parts.length > 1
        ? new DBObject[] { (DBObject) JSON.parse( parts[1] ) }
        : new DBObject[0];
    when( mongoCollectionWrapper.aggregate( dbObjQuery, remainder ) ).thenReturn( results );
    Iterable iterableResults = mock( Iterable.class );
    Iterator resultsIterator = mock( Iterator.class );
    when( iterableResults.iterator() ).thenReturn( resultsIterator );
    when( results.results() ).thenReturn( iterableResults );

    dbInput.init( stepMetaInterface, stepDataInterface );
    dbInput.processRow( stepMetaInterface, stepDataInterface );

    verify( stepDataInterface ).init();
    verify( mongoCollectionWrapper ).aggregate( dbObjQuery, remainder );
    assertEquals( stepDataInterface.m_pipelineResult, resultsIterator );
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
    assertThat( putRow[0], CoreMatchers.<Object>equalTo( nextDoc.toString() ) );
  }

  @Test public void testFindWithQuery() throws KettleException, MongoDbException {
    // no query or fields defined, should do a collection.find()
    setupReturns();
    String query = "{ type : 'heavy'} ";
    when( stepMetaInterface.getJsonQuery() ).thenReturn( query );
    dbInput.init( stepMetaInterface, stepDataInterface );
    assertFalse( "should return false as there are no more results",
        dbInput.processRow( stepMetaInterface, stepDataInterface ) );
    verify( mongoCollectionWrapper ).find( dbObjectCaptor.capture(), any( DBObject.class ) );
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
    verify( mongoCollectionWrapper ).find( dbObjectCaptor.capture(), any( DBObject.class ) );
    assertThat( dbObjectCaptor.getValue(),
        equalTo( (DBObject) JSON.parse( "{foo : 'bar'}" ) ) );
  }

}
