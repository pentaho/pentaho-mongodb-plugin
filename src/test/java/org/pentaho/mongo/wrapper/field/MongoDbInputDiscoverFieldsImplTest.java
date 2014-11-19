package org.pentaho.mongo.wrapper.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongoDbInputDiscoverFieldsImplTest {

  @Test
  public void testSetMinArrayIndexesNoArraysPresent() {
    MongoField m = new MongoField();
    m.m_fieldName = "bob.fred.george";
    m.m_fieldPath = "bob.fred.george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes( m );
    assertEquals( "bob.fred.george", m.m_fieldName );
    assertEquals( "bob.fred.george", m.m_fieldPath );
  }

  @Test
  public void testSetMinArrayIndexesOneArray() {
    MongoField m = new MongoField();
    m.m_fieldName = "bob.fred[2:10].george";
    m.m_fieldPath = "bob.fred[-].george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes( m );
    assertEquals( "bob.fred[2].george", m.m_fieldPath );
  }

  @Test
  public void testSetMinArrayIndexesTwoArrays() {
    MongoField m = new MongoField();
    m.m_fieldName = "bob[5:5].fred[2:10].george";
    m.m_fieldPath = "bob[-].fred[-].george";

    MongodbInputDiscoverFieldsImpl.setMinArrayIndexes( m );
    assertEquals( "bob[5].fred[2].george", m.m_fieldPath );
  }

  @Test
  public void testUpdateMinMaxArrayIndexes() {

    MongoField m = new MongoField();
    m.m_fieldName = "bob.fred[2:4].george";
    m.m_fieldPath = "bob.fred[-].george";

    MongodbInputDiscoverFieldsImpl.updateMinMaxArrayIndexes( m, "bob.fred[1:1].george" );

    assertEquals( "bob.fred[1:4].george", m.m_fieldName );
    MongodbInputDiscoverFieldsImpl.updateMinMaxArrayIndexes( m, "bob.fred[5:5].george" );
    assertEquals( "bob.fred[1:5].george", m.m_fieldName );
  }

  @Test
  public void testPostProcessPaths() {
    Map<String, MongoField> fieldMap = new LinkedHashMap<String, MongoField>();
    List<MongoField> discovered = new ArrayList<MongoField>();

    MongoField m = new MongoField();
    m.m_fieldPath = "bob.fred[-].george";
    m.m_fieldName = "bob.fred[2:10].george";
    m.m_percentageOfSample = 5;
    fieldMap.put( m.m_fieldPath, m );
    m = new MongoField();
    m.m_fieldPath = "one.two[-]";
    m.m_fieldName = "one.two[1]";
    m.m_percentageOfSample = 10;
    fieldMap.put( m.m_fieldPath, m );

    MongodbInputDiscoverFieldsImpl.postProcessPaths( fieldMap, discovered, 100 );

    assertEquals( 2, discovered.size() );
    m = discovered.get( 0 );
    assertEquals( "george", m.m_fieldName );
    assertEquals( "bob.fred[2].george", m.m_fieldPath );
    assertEquals( "5/100", m.m_occurenceFraction );
    assertEquals( "bob.fred[2:10].george", m.m_arrayIndexInfo );

    m = discovered.get( 1 );
    assertEquals( "two[1]", m.m_fieldName );
    assertEquals( "one.two[1]", m.m_fieldPath );
    assertEquals( "10/100", m.m_occurenceFraction );
    assertEquals( null, m.m_arrayIndexInfo );
  }

  @Test
  public void testDocToFields() {
    Map<String, MongoField> fieldMap = new LinkedHashMap<String, MongoField>();
    DBObject doc = (DBObject) JSON.parse( "{\"fred\" : {\"george\" : 1}, \"bob\" : [1 , 2]}" );

    MongodbInputDiscoverFieldsImpl.docToFields( doc, fieldMap );
    assertEquals( 3, fieldMap.size() );

    assertTrue( fieldMap.get( "$.fred.george" ) != null );
    assertTrue( fieldMap.get( "$.bob[0]" ) != null );
    assertTrue( fieldMap.get( "$.bob[1]" ) != null );
    assertTrue( fieldMap.get( "$.bob[2]" ) == null );
  }
}
