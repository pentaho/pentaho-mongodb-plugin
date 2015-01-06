package org.pentaho.di.trans.steps.mongodbinput;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.trans.steps.mongodbinput.MongoDbInputDialog;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class MongoDbInputDataTest {
  private MongoDbInputData mongoDbInputData;

  @Before
  public void setUp() {
    mongoDbInputData = new MongoDbInputData();
  }

  @Test
  public void testDiscoverFields() throws Exception {
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
        ( (DiscoverFieldsCallback) invocationOnMock.getArguments()[8] ).notifyFields( mongoFields );
        return null;
      }
    }).when( mongoDbInputDiscoverFields ).discoverFields( any( MongoProperties.Builder.class ), anyString(),
      anyString(),
      anyString(), anyString(), anyBoolean(), anyInt(), any( MongoDbInputMeta.class ),
      any( DiscoverFieldsCallback.class ) );

    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    mongoDbInputData.discoverFields( meta, vars, docsToSample, dialog );
    verify( holder, atLeastOnce() ).getMongoDbInputDiscoverFields();
  }

  @Test
  public void testDiscoverFieldsWithoutCallback() throws Exception {
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
    when( mongoDbInputDiscoverFields.discoverFields( any( MongoProperties.Builder.class ), anyString(), anyString(),
      anyString(), anyString(), anyBoolean(), anyInt(), any( MongoDbInputMeta.class ) ) ).thenReturn( mongoFields );
    when( holder.getMongoDbInputDiscoverFields() ).thenReturn( mongoDbInputDiscoverFields );
    mongoDbInputData.setMongoDbInputDiscoverFieldsHolder( holder );

    boolean result = mongoDbInputData.discoverFields( meta, vars, docsToSample );

    assertTrue( result );
  }
}
