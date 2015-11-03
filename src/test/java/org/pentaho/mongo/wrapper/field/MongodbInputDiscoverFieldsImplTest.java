/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.mongo.wrapper.field;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoDBAction;
import org.pentaho.mongo.wrapper.MongoWrapperClientFactory;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class MongodbInputDiscoverFieldsImplTest {

  @Mock private MongoWrapperClientFactory clientFactory;
  @Mock private MongoClientWrapper clientWrapper;
  @Mock private DB mockDb;
  @Mock private MongoDbInputMeta inputMeta;
  @Mock private DBCollection collection;
  @Mock private DBCursor cursor;
  @Captor private ArgumentCaptor<MongoProperties.Builder> propCaptor;
  @Captor private ArgumentCaptor<DBObject> dbObjectCaptor;
  @Captor private ArgumentCaptor<DBObject[]> dbObjectArrayCaptor;

  private MongodbInputDiscoverFieldsImpl discoverFields = new MongodbInputDiscoverFieldsImpl();
  private final int NUM_DOCS_TO_SAMPLE = 2;

  @Before public void before() throws MongoDbException, KettlePluginException {
    MockitoAnnotations.initMocks( this );
    MongoWrapperUtil.setMongoWrapperClientFactory( clientFactory );
    when( clientFactory.createMongoClientWrapper( any( MongoProperties.class ), any( MongoUtilLogger.class ) ) )
        .thenReturn( clientWrapper );
    when( mockDb.getCollection( any( String.class ) ) ).thenReturn( collection );
    when( collection.find() ).thenReturn( cursor );
    when( cursor.limit( anyInt() ) ).thenReturn( cursor );
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
  }

  @Test public void testDiscoverFieldsSimpleDoc() throws Exception {
    setupPerform();
    BasicDBObject doc = new BasicDBObject();
    doc.put( "foo", "bar" );
    doc.put( "baz", 123 );
    when( cursor.next() ).thenReturn( doc );
    List<MongoField> fields =
        discoverFields
            .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
                inputMeta );
    validateFields( fields, "baz", "baz", 123l, // f1 name, path, value
        "foo", "foo", "bar" ); // f2 name, path, value
  }

  @Test public void testDiscoverFieldsNameCollision() throws Exception {
    setupPerform();
    BasicDBObject doc = new BasicDBObject();
    doc.put( "foo", "bar" );
    doc.put( "baz", new BasicDBObject( "foo", "bop" ) );
    when( cursor.next() ).thenReturn( doc );
    List<MongoField> fields =
        discoverFields
            .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
                inputMeta );
    validateFields( fields,
        "foo", "baz.foo", "stringVal",
        "foo_1", "foo", "stringVal" );
  }

  @Test public void testDiscoverFieldsNestedArray() throws Exception {
    setupPerform();

    BasicDBObject doc = new BasicDBObject();
    BasicDBList list = new BasicDBList();
    list.add( new BasicDBObject( "bar", BigDecimal.valueOf( 123.123 ) ) );
    Date date = new Date();
    list.add( new BasicDBObject( "fap",  date ) );
    doc.put( "foo", list );
    doc.put( "baz", new BasicDBObject( "bop", new BasicDBObject( "fop", false ) ) );
    when( cursor.next() ).thenReturn( doc );
    List<MongoField> fields =
        discoverFields
            .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
                inputMeta );
    validateFields( fields,
        "bar", "foo.0.bar", 123.123,           // field 0
        "fap", "foo.1.fap", date,              // field 1
        "fop", "baz.bop.fop", "stringValue" ); // field 2
  }

  @Test public void testDiscoverFieldsNestedDoc() throws Exception {
    setupPerform();

    BasicDBObject doc = new BasicDBObject();
    doc.put( "foo", new BasicDBObject( "bar", BigDecimal.valueOf( 123.123 ) ) );
    doc.put( "baz", new BasicDBObject( "bop", new BasicDBObject( "fop", false ) ) );
    when( cursor.next() ).thenReturn( doc );
    List<MongoField> fields =
        discoverFields
            .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
                inputMeta );
    validateFields( fields,
        "bar", "foo.bar", 123.123,
        "fop", "baz.bop.fop", "stringValue" );
  }

  @Test public void testArraysInArrays() throws MongoDbException, KettleException {
    setupPerform();

    DBObject doc = (DBObject) JSON.parse(
        "{ top : [ { parentField1 :  "
            + "[ 'nested1', 'nested2']   },"
            + " {parentField2 : [ 'nested3' ] } ] }" );
    when( cursor.next() ).thenReturn( doc );
    List<MongoField> fields =
        discoverFields
            .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
                inputMeta );
    validateFields( fields, "parentField1[0]", "top[0:0].parentField1.0", "stringVal", "parentField1[1]",
        "top[0:0].parentField1.1", "stringVal", "parentField2[0]", "top[1:1].parentField2.0", "stringVal" );
  }

  @Test public void testPipelineQueryIsLimited() throws KettleException, MongoDbException {
    setupPerform();

    AggregationOutput aggOutput = mock( AggregationOutput.class );
    Iterable<DBObject> results = mock( Iterable.class );
    when( aggOutput.results() ).thenReturn( results );
    when( results.iterator() ).thenReturn( mock( Iterator.class ) );

    String query = "{$sort : 1}";
    DBObject firstOp = (DBObject) JSON.parse( query );
    DBObject[] remainder = { new BasicDBObject( "$limit", NUM_DOCS_TO_SAMPLE ) };
    when( collection.aggregate( firstOp, remainder ) )
        .thenReturn( aggOutput );

    discoverFields.discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", query, "", true,
        NUM_DOCS_TO_SAMPLE, inputMeta );

    verify( collection ).aggregate( firstOp, remainder );
  }

  @Test ( expected = KettleException.class )
  public void testClientExceptionIsRethrown() throws MongoDbException, KettleException {
    when( clientFactory.createMongoClientWrapper( any( MongoProperties.class ), any( MongoUtilLogger.class ) ) )
        .thenThrow( mock( MongoDbException.class ) );
    setupPerform();
    discoverFields
        .discoverFields( new MongoProperties.Builder(), "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
            inputMeta );
  }

  @Test ( expected = KettleException.class )
  public void testExceptionRetrievingCollectionIsRethrown() throws MongoDbException, KettleException {
    when( mockDb.getCollection( any( String.class ) ) )
        .thenThrow( mock( RuntimeException.class ) );
    setupPerform();
    discoverFields
        .discoverFields( new MongoProperties.Builder(),
            "mydb", "mycollection", "", "", false, NUM_DOCS_TO_SAMPLE,
            inputMeta );
  }

  private void setupPerform() throws MongoDbException {
    when( clientWrapper.perform( any( String.class ), any( MongoDBAction.class ) ) )
        .thenAnswer( new Answer<List<MongoField>>() {
          @Override public List<MongoField> answer( InvocationOnMock invocationOnMock ) throws Throwable {
            MongoDBAction action = (MongoDBAction) invocationOnMock.getArguments()[1];
            return (List<MongoField>) action.perform( mockDb );
          }
        } );
    setupCursorWithNRows( NUM_DOCS_TO_SAMPLE );
  }

  private void setupCursorWithNRows( final int N ) {
    when( cursor.hasNext() ).thenAnswer( new Answer<Boolean>() {
      int count = 0;
      @Override public Boolean answer( InvocationOnMock invocationOnMock ) throws Throwable {
        return count++ < N;
      }
    } );
  }

  /**
   * Checks that each field has the expected trio of name, path, and kettlevalue
   * contained in the expecteds vararg.  The expecteds should contain an array of
   * { nameForField1, pathForField1, valueForField1,
   *   nameForField2, pathForField2, valueForField2, ... }
   */
  private void validateFields( List<MongoField> fields, Object... expecteds  ) throws KettleException {
    assertThat( expecteds.length, equalTo( fields.size()  * 3 ) );
    Collections.sort( fields );
    for ( int i = 0; i < fields.size(); i++ ) {
      fields.get( i ).init( i );
      assertThat( fields.get( i ).getName(), equalTo( expecteds[i * 3] ) );
      assertThat( fields.get( i ).getPath(), equalTo( expecteds[ i * 3 + 1 ] ) );
      assertThat( fields.get( i ).getKettleValue( expecteds[ i * 3 + 2 ] ),
          equalTo( expecteds[ i * 3 + 2 ] ) );
    }
  }

}
