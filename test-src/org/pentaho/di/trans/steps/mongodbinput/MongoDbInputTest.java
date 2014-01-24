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

package org.pentaho.di.trans.steps.mongodbinput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.mongo.wrapper.field.MongoField;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongoDbInputTest {
  protected static String s_testData = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } }, "
      + "{ \"rec1\" : { \"f1\" : \"sid\", \"f2\" : \"zaphod\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";

  static {
    try {
      ValueMetaPluginType.getInstance().searchPlugins();
    } catch (KettlePluginException ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testGetNonExistentField() throws KettleException {
    Object mongoO = JSON.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.iDontExist";
    mm.m_kettleType = "String";
    discoveredFields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoField m : discoveredFields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(discoveredFields);
    data.init();
    Variables vars = new Variables();
    Object[] result = data.mongoDocumentToKettle((DBObject) mongoO, vars)[0];

    assertTrue(result != null);
    assertEquals(1, result.length - RowDataUtil.OVER_ALLOCATE_SIZE);
    assertTrue(result[0] == null);
  }

  @Test
  public void testArrayUnwindArrayFieldsOnly() throws KettleException {
    Object mongoO = JSON.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";

    fields.add(mm);
    RowMetaInterface rowMeta = new RowMeta();
    for (MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);
  }

  @Test
  public void testArrayUnwindOneArrayExpandFieldAndOneNormalField()
      throws KettleException {
    Object mongoO = JSON.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add(mm);

    mm = new MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.name";
    mm.m_kettleType = "String";
    fields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // george should be the name in both rows
    assertEquals("george", result[0][1]);
    assertEquals("george", result[1][1]);
  }

  @Test
  public void testArrayUnwindWithOneExistingAndOneNonExistingField()
      throws KettleException {
    Object mongoO = JSON.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);

    List<MongoField> fields = new ArrayList<MongoField>();

    MongoField mm = new MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add(mm);

    mm = new MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.one.two[*].rec6.nonExistent";
    mm.m_kettleType = "String";
    fields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // this field doesn't exist in the doc structure, so we expect null
    assertTrue(result[0][1] == null);
    assertTrue(result[1][1] == null);
  }

  public static void main(String[] args) {
    MongoDbInputTest test = new MongoDbInputTest();
    try {
      test.testGetNonExistentField();
      test.testArrayUnwindArrayFieldsOnly();
      test.testArrayUnwindWithOneExistingAndOneNonExistingField();
      test.testArrayUnwindOneArrayExpandFieldAndOneNormalField();
    } catch (KettleException e) {
      e.printStackTrace();
    }
  }
}
