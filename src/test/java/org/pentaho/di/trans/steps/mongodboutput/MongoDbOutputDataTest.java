/*!
 * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MongoDbOutputDataTest {

  @Mock private VariableSpace space;
  @Mock private MongoClientWrapper client;
  @Mock private MongoCollectionWrapper collection;
  @Mock private RowMetaInterface rowMeta;
  @Mock private ValueMetaInterface valueMeta;

  @Before
  public void before() {
    MockitoAnnotations.initMocks( this );
    when( space.environmentSubstitute( any( String.class ) ) )
        .thenAnswer( new Answer<String>() {
          @Override public String answer( InvocationOnMock invocationOnMock ) throws Throwable {
            return (String) invocationOnMock.getArguments()[0];
          }
        } );
  }

  @BeforeClass
  public static void setUpBeforeClass() throws KettleException {
    KettleEnvironment.init( false );
  }

  @Test
  public void testApplyIndexesOptions() throws KettleException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    LogChannelInterface log = new LogChannel( data );
    DBCollection collection = mock( DBCollection.class );
    MongoCollectionWrapper collectionWrapper = spy( new DefaultMongoCollectionWrapper( collection ) );
    data.setCollection( collectionWrapper );

    ArgumentCaptor<BasicDBObject> captorIndexes = ArgumentCaptor.forClass( BasicDBObject.class );
    ArgumentCaptor<BasicDBObject> captorOptions = ArgumentCaptor.forClass( BasicDBObject.class );
    doNothing().when( collectionWrapper ).createIndex( captorIndexes.capture(), captorOptions.capture() );

    MongoIndex index = new MongoDbOutputMeta.MongoIndex();
    index.m_pathToFields = "FirstName:1";
    index.m_drop = false;
    index.m_sparse = false;
    index.m_unique = false;

    // Test with all options false
    data.applyIndexes( Arrays.asList( index ), log, false );
    BasicDBObject createdIndex = captorIndexes.getValue();
    BasicDBObject createdOptions = captorOptions.getValue();

    assertEquals( 1, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );
    assertTrue( createdOptions.containsField( "background" ) );
    assertEquals( true, createdOptions.getBoolean( "background" ) );
    assertTrue( createdOptions.containsField( "sparse" ) );
    assertEquals( false, createdOptions.getBoolean( "sparse" ) );
    assertTrue( createdOptions.containsField( "unique" ) );
    assertEquals( false, createdOptions.getBoolean( "unique" ) );

    // Test with only "sparse" true
    index.m_sparse = true;
    index.m_unique = false;
    data.applyIndexes( Arrays.asList( index ), log, false );
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals( 1, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );
    assertTrue( createdOptions.containsField( "background" ) );
    assertEquals( true, createdOptions.getBoolean( "background" ) );
    assertTrue( createdOptions.containsField( "sparse" ) );
    assertEquals( true, createdOptions.getBoolean( "sparse" ) );
    assertTrue( createdOptions.containsField( "unique" ) );
    assertEquals( false, createdOptions.getBoolean( "unique" ) );

    // Test with only "unique" true
    index.m_sparse = false;
    index.m_unique = true;
    data.applyIndexes( Arrays.asList( index ), log, false );
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals( 1, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );
    assertTrue( createdOptions.containsField( "background" ) );
    assertEquals( true, createdOptions.getBoolean( "background" ) );
    assertTrue( createdOptions.containsField( "sparse" ) );
    assertEquals( false, createdOptions.getBoolean( "sparse" ) );
    assertTrue( createdOptions.containsField( "unique" ) );
    assertEquals( true, createdOptions.getBoolean( "unique" ) );

    // Test with "sparse" and "unique" true
    index.m_sparse = true;
    index.m_unique = true;
    data.applyIndexes( Arrays.asList( index ), log, false );
    createdIndex = captorIndexes.getValue();
    createdOptions = captorOptions.getValue();

    assertEquals( 1, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );
    assertTrue( createdOptions.containsField( "background" ) );
    assertEquals( true, createdOptions.getBoolean( "background" ) );
    assertTrue( createdOptions.containsField( "sparse" ) );
    assertEquals( true, createdOptions.getBoolean( "sparse" ) );
    assertTrue( createdOptions.containsField( "unique" ) );
    assertEquals( true, createdOptions.getBoolean( "unique" ) );
  }

  @Test
  public void testApplyIndexesSplits() throws KettleException, MongoDbException {
    MongoDbOutputData data = new MongoDbOutputData();
    LogChannelInterface log = new LogChannel( data );
    DBCollection collection = mock( DBCollection.class );
    MongoCollectionWrapper collectionWrapper = spy( new DefaultMongoCollectionWrapper( collection ) );
    data.setCollection( collectionWrapper );

    ArgumentCaptor<BasicDBObject> captorIndexes = ArgumentCaptor.forClass( BasicDBObject.class );
    doNothing().when( collectionWrapper ).createIndex( captorIndexes.capture(), any( BasicDBObject.class ) );

    MongoIndex index = new MongoDbOutputMeta.MongoIndex();
    index.m_pathToFields = "FirstName:1";
    index.m_drop = false;
    index.m_sparse = false;
    index.m_unique = false;

    data.applyIndexes( Arrays.asList( index ), log, false );
    BasicDBObject createdIndex = captorIndexes.getValue();
    assertEquals( 1, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );

    //Test multiple fields
    index.m_pathToFields = "FirstName:1,LastName:-1,Street:1";
    data.applyIndexes( Arrays.asList( index ), log, false );
    createdIndex = captorIndexes.getValue();
    assertEquals( 3, createdIndex.size() );
    assertTrue( createdIndex.containsField( "FirstName" ) );
    assertEquals( "1", createdIndex.getString( "FirstName" ) );
    assertTrue( createdIndex.containsField( "LastName" ) );
    assertEquals( "-1", createdIndex.getString( "LastName" ) );
    assertTrue( createdIndex.containsField( "Street" ) );
    assertEquals( "1", createdIndex.getString( "Street" ) );
  }

  @Test
  public void testSetInitGet() throws KettleException {
    // validates setting, initializing, and getting of MongoFields.
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    MongoDbOutputMeta.MongoField field2 = new MongoDbOutputMeta.MongoField();
    field1.m_incomingFieldName = "field1";
    field1.m_mongoDocPath = "parent.field1";
    field2.m_incomingFieldName = "field2";
    field2.m_mongoDocPath = "parent.field2";

    MongoDbOutputData data = new MongoDbOutputData();
    data.setMongoFields( Arrays.asList( field1, field2 ) );
    data.init( space );

    List<MongoDbOutputMeta.MongoField> fields = data.getMongoFields();
    assertThat( fields.size(), equalTo( 2 ) );
    assertThat( fields.get( 0 ).m_incomingFieldName, equalTo( "field1" ) );
    assertThat( fields.get( 1 ).m_incomingFieldName, equalTo( "field2" ) );
    assertThat( fields.get( 0 ).m_pathList, equalTo( Arrays.asList( "parent", "field1" ) ) );
    assertThat( fields.get( 1 ).m_pathList, equalTo( Arrays.asList( "parent", "field2" ) ) );
  }

  @Test
  public void testGetQueryObjectWithIncomingJson() throws KettleException {
    MongoDbOutputMeta.MongoField field1 = new MongoDbOutputMeta.MongoField();
    field1.m_JSON = true;
    field1.m_updateMatchField = true;
    when( rowMeta.getValueMeta( anyInt() ) )
        .thenReturn( valueMeta );
    String query = "{ foo : 'bar' }";
    when( valueMeta.getString( any( Object[].class ) ) )
        .thenReturn( query );
    Object[] row = new Object[] { "foo" };

    when( valueMeta.isString() ).thenReturn( false );
    try {
      MongoDbOutputData
          .getQueryObject( Arrays.asList( field1 ), rowMeta, row, space, MongoDbOutputData.MongoTopLevel.RECORD );
      fail( "expected an exception, can't construct query from non-string." );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( KettleException.class ) );
    }

    when( valueMeta.isString() ).thenReturn( true );
    assertThat( MongoDbOutputData
            .getQueryObject( Arrays.asList( field1 ), rowMeta, row, space, MongoDbOutputData.MongoTopLevel.RECORD ),
        equalTo( (DBObject) JSON.parse( query ) ) );


  }

  @Test
  public void testWrapperMethods() {
    MongoDbOutputData data = new MongoDbOutputData();
    data.setConnection( client );
    assertThat( data.getConnection(), equalTo( client ) );
    data.setCollection( collection );
    assertThat( data.getCollection(), equalTo( collection ) );
    data.setOutputRowMeta( rowMeta );
    assertThat( data.getOutputRowMeta(), equalTo( rowMeta ) );
  }

}
