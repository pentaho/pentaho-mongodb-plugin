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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta.MongoIndex;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class MongoDbOutputDataTest {

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
}
