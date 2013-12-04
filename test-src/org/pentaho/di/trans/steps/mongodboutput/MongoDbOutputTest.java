package org.pentaho.di.trans.steps.mongodboutput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputData.kettleRowToMongo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.easymock.internal.ClassInstantiatorFactory;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.fuzzymatch.FuzzyMatchData;
import org.pentaho.di.trans.steps.fuzzymatch.FuzzyMatchMeta;

import com.mongodb.DBObject;


/**
 * Unit tests for MongoDbOutput
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MongoDbOutputTest {

  @Test
  public void testTopLevelObjectStructureNoNestedDocs() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD, false);

    assertEquals(result.toString(),
        "{ \"field1\" : \"value1\" , \"field2\" : 12}");

  }

  @Test
  public void testTopLevelArrayStructureWithPrimitives() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[1]";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.ARRAY, false);

    assertEquals(result.toString(), "[ \"value1\" , 12]");
  }

  @Test
  public void testTopLevelArrayStructureWithObjects() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[1]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.ARRAY, false);

    assertEquals(result.toString(),
        "[ { \"field1\" : \"value1\"} , { \"field2\" : 12}]");
  }

  @Test
  public void testTopLevelArrayStructureContainingOneObjectMutipleFields()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.ARRAY, false);

    assertEquals(result.toString(),
        "[ { \"field1\" : \"value1\" , \"field2\" : 12}]");
  }

  @Test
  public void testTopLevelArrayStructureContainingObjectWithArray()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0].inner[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[0].inner[1]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.ARRAY, false);

    assertEquals(result.toString(),
        "[ { \"inner\" : [ { \"field1\" : \"value1\"} , { \"field2\" : 12}]}]");
  }

  @Test
  public void testTopLevelObjectStructureOneLevelNestedDoc()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD, false);

    assertEquals(result.toString(),
        "{ \"field1\" : \"value1\" , \"nestedDoc\" : { \"field2\" : 12}}");
  }

  @Test
  public void testTopLevelObjectStructureTwoLevelNested()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "nestedDoc.secondNested";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);

    Object[] row = new Object[2];
    row[0] = "value1";
    row[1] = new Long(12);
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD, false);

    assertEquals(
        result.toString(),
        "{ \"nestedDoc\" : { \"secondNested\" : { \"field1\" : \"value1\"} , \"field2\" : 12}}");
  }

  @Test
  public void testModifierUpdateWithMultipleModifiersOfSameType()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();
    MongoDbOutputData data = new MongoDbOutputData();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    RowMetaInterface rm = new RowMeta();
    ValueMetaInterface vm = new ValueMeta("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);
    vm = new ValueMeta("field2");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);

    VariableSpace vars = new Variables();
    Object[] dummyRow = new Object[] { "value1", "value2" };

    // test to see that having more than one path specify the $set operation
    // results
    // in an update object with two entries
    DBObject modifierUpdate = data.getModifierUpdateObject(paths, rm, dummyRow,
        vars, MongoDbOutputData.MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$set") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$set");
    assertEquals(setOpp.keySet().size(), 2);
  }

  @Test
  public void testModifierSetComplexArrayGrouping() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();
    MongoDbOutputData data = new MongoDbOutputData();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "bob.fred[0].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "bob.fred[0].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$set";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    RowMetaInterface rm = new RowMeta();
    ValueMetaInterface vm = new ValueMeta("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);
    vm = new ValueMeta("field2");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);

    VariableSpace vars = new Variables();
    Object[] dummyRow = new Object[] { "value1", "value2" };

    DBObject modifierUpdate = data.getModifierUpdateObject(paths, rm, dummyRow,
        vars, MongoDbOutputData.MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$set") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$set");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" as the first
    // entry in
    // the array - so there should be one entry for $set
    assertEquals(setOpp.keySet().size(), 1);

    // check the resulting update structure
    assertEquals(
        modifierUpdate.toString(),
        "{ \"$set\" : { \"bob.fred\" : [ { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\"}}]}}");
  }

  @Test
  public void testModifierPushComplexObject() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();
    MongoDbOutputData data = new MongoDbOutputData();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "bob.fred[].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "bob.fred[].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    RowMetaInterface rm = new RowMeta();
    ValueMetaInterface vm = new ValueMeta("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);
    vm = new ValueMeta("field2");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);

    VariableSpace vars = new Variables();
    Object[] dummyRow = new Object[] { "value1", "value2" };

    DBObject modifierUpdate = data.getModifierUpdateObject(paths, rm, dummyRow,
        vars, MongoDbOutputData.MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$push") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$push");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" for $push to
    // append
    // to the end of the array
    assertEquals(setOpp.keySet().size(), 1);

    assertEquals(
        modifierUpdate.toString(),
        "{ \"$push\" : { \"bob.fred\" : { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\"}}}}");
  }

  @Test
  public void testModifierPushComplexObjectWithJsonNestedDoc()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();
    MongoDbOutputData data = new MongoDbOutputData();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "bob.fred[].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "bob.fred[].george";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierUpdateOperation = "$push";
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "jsonField";
    mf.m_mongoDocPath = "bob.fred[].george";
    mf.m_modifierUpdateOperation = "$push";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_modifierOperationApplyPolicy = "Insert&Update";
    mf.m_JSON = true;
    paths.add(mf);

    RowMetaInterface rm = new RowMeta();
    ValueMetaInterface vm = new ValueMeta("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);
    vm = new ValueMeta("field2");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("jsonField");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rm.addValueMeta(vm);

    VariableSpace vars = new Variables();
    Object[] dummyRow = new Object[] { "value1", "value2",
        "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}" };

    DBObject modifierUpdate = data.getModifierUpdateObject(paths, rm, dummyRow,
        vars, MongoDbOutputData.MongoTopLevel.RECORD);

    assertTrue(modifierUpdate != null);
    assertTrue(modifierUpdate.get("$push") != null);
    DBObject setOpp = (DBObject) modifierUpdate.get("$push");

    // in this case, we have the same path up to the array (bob.fred). The
    // remaining
    // terminal fields should be grouped into one record "george" for $push to
    // append
    // to the end of the array
    assertEquals(setOpp.keySet().size(), 1);

    assertEquals(
        modifierUpdate.toString(),
        "{ \"$push\" : { \"bob.fred\" : { \"george\" : { \"field1\" : \"value1\" , \"field2\" : \"value2\" , \"jsonField\" : "
            + "{ \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}}}}}");
  }

  @Test
  public void testInsertKettleFieldThatContainsJsonIntoTopLevelRecord()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "jsonField";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_JSON = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("jsonField");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = new Long(12);
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD, false);

    assertEquals(
        result.toString(),
        "{ \"field1\" : \"value1\" , \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}}");

  }

  @Test
  public void testScanForInsertTopLevelJSONDocAsIs() throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    assertTrue(MongoDbOutputData.scanForInsertTopLevelJSONDoc(paths));
  }

  @Test
  public void testForInsertTopLevelJSONDocAsIsWithOneJSONMatchPathAndOneJSONInsertPath()
      throws KettleException {

    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "";
    mf.m_mongoDocPath = "";
    mf.m_updateMatchField = true;
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    assertTrue(MongoDbOutputData.scanForInsertTopLevelJSONDoc(paths));
  }

  @Test
  public void testScanForInsertTopLevelJSONDocAsIsWithMoreThanOnePathSpecifyingATopLevelJSONDocToInsert() {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    try {
      MongoDbOutputData.scanForInsertTopLevelJSONDoc(paths);
      fail("Was expecting an exception because more than one path specifying a JSON "
          + "doc to insert as-is is not kosher");
    } catch (KettleException ex) {
      // an Exception is expected
    }
  }

  @Test
  public void testInsertKettleFieldThatContainsJsonIntoOneLevelNestedDoc()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "jsonField";
    mf.m_mongoDocPath = "nestedDoc";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_JSON = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("jsonField");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = new Long(12);
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD, false);

    assertEquals(
        result.toString(),
        "{ \"field1\" : \"value1\" , \"nestedDoc\" : { \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}}}");
  }

  @Test
  public void testInsertKettleFieldThatContainsJsonIntoTopLevelArray()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "[0]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "[1]";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "jsonField";
    mf.m_mongoDocPath = "[2]";
    mf.m_useIncomingFieldNameAsMongoFieldName = false;
    mf.m_JSON = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("jsonField");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = new Long(12);
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject result = kettleRowToMongo(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.ARRAY, false);

    assertEquals(
        result.toString(),
        "[ { \"field1\" : \"value1\"} , { \"field2\" : 12} , { \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}]");

  }

  @Test
  public void testGetQueryObjectThatContainsJsonNestedDoc()
      throws KettleException {
    List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

    MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field1";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_updateMatchField = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "field2";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_updateMatchField = true;
    paths.add(mf);

    mf = new MongoDbOutputMeta.MongoField();
    mf.m_incomingFieldName = "jsonField";
    mf.m_mongoDocPath = "";
    mf.m_useIncomingFieldNameAsMongoFieldName = true;
    mf.m_updateMatchField = true;
    mf.m_JSON = true;
    paths.add(mf);

    RowMetaInterface rmi = new RowMeta();
    ValueMetaInterface vm = new ValueMeta();
    vm.setName("field1");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("field2");
    vm.setType(ValueMetaInterface.TYPE_INTEGER);
    rmi.addValueMeta(vm);
    vm = new ValueMeta();
    vm.setName("jsonField");
    vm.setType(ValueMetaInterface.TYPE_STRING);
    rmi.addValueMeta(vm);

    Object[] row = new Object[3];
    row[0] = "value1";
    row[1] = new Long(12);
    row[2] = "{\"jsonDocField1\" : \"aval\", \"jsonDocField2\" : 42}";
    VariableSpace vs = new Variables();

    for (MongoDbOutputMeta.MongoField f : paths) {
      f.init(vs);
    }

    DBObject query = MongoDbOutputData.getQueryObject(paths, rmi, row, vs,
        MongoDbOutputData.MongoTopLevel.RECORD);

    assertEquals(
        query.toString(),
        "{ \"field1\" : \"value1\" , \"field2\" : 12 , \"jsonField\" : { \"jsonDocField1\" : \"aval\" , \"jsonDocField2\" : 42}}");
  }

  /**
   * PDI-9989. If step configuration contains mongo field names which doesn't have equivalents
   * in step input row meta - throw exception, with detailed information.
   * 
   * (expected mongo field name b1 has no equivalent in input row meta)
   * @throws KettleException
   */
  @Test( expected = KettleException.class )
  public void testStepIsFailedIfOneOfMongoFieldsNotFound() throws KettleException{
	MongoDbOutput output = prepareMongoDbOutputMock();

	final String[] metaNames = new String[]{ "a1", "a2", "a3" };
	String[] mongoNames = new String[]{ "b1", "a2", "a3" };
	List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields( mongoNames );
	RowMetaInterface rmi = getStubRowMetaInterface( metaNames );
	EasyMock.replay( output );
	try{
		output.checkInputFieldsMatch(rmi, mongoFields);
	} catch ( KettleException e ){
		String detailedMessage = e.getMessage();
		Assert.assertTrue("Exception message mentions fields not found:", detailedMessage.contains( "b1" ));
		throw new KettleException( e );	
	}
  }
  
  /**
   * Tests if mongo output configuration contains excessive fields in step input
   * against mongo output fields, we generate a log record about the fields  
   * will not be used in mongo output.
   * @throws KettleException
   */
  @Test
  public void testStepLogSkippedFields() throws KettleException{
	MongoDbOutput output = prepareMongoDbOutputMock();

	final String[] metaNames = new String[]{ "a1", "a2", "a3" };
	String[] mongoNames = new String[]{ "a1", "a2" };
	Capture<String> loggerCapture = new Capture<String>( CaptureType.ALL );
	output.logBasic( EasyMock.capture( loggerCapture ) );
	EasyMock.replay( output );
	RowMetaInterface rmi = getStubRowMetaInterface( metaNames );
	List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields( mongoNames );
	
	output.checkInputFieldsMatch(rmi, mongoFields);	  
	List<String> logRecords = loggerCapture.getValues();

	Assert.assertEquals("We have one log record generated", 1, logRecords.size() );
	Assert.assertTrue("We have a log record mentions that 'a3' field will not be used.", 
			logRecords.get( 0 ).contains( "a3" ));
  }
  
  /**
   * Tests if none mongo output fields exists - step is failed.
   * @throws KettleException
   */
  @Test( expected = KettleException.class )
  public void testStepFailsIfNoMongoFieldsFound() throws KettleException{
	MongoDbOutput output = prepareMongoDbOutputMock();

	final String[] metaNames = new String[]{ "a1", "a2" };
	String[] mongoNames = new String[]{  };
	EasyMock.replay( output );
	RowMetaInterface rmi = getStubRowMetaInterface( metaNames );
	List<MongoDbOutputMeta.MongoField> mongoFields = getMongoFields( mongoNames );
	output.checkInputFieldsMatch(rmi, mongoFields);
  }
  

  private MongoDbOutput prepareMongoDbOutputMock(){  
	  MongoDbOutput output = EasyMock.createNiceMock( MongoDbOutput.class );
	  EasyMock.expect( output.environmentSubstitute( EasyMock.anyObject( String.class ) ) )
			  .andAnswer(new IAnswer<String>(){
		@Override
		public String answer() throws Throwable {
			Object[] args = EasyMock.getCurrentArguments();
			String ret = String.valueOf( args[0] );
			return ret;
		}
	  } ).anyTimes();
	  return output;
  }
  
  private RowMetaInterface getStubRowMetaInterface( final String[] metaNames ){
	  RowMetaInterface rmi = EasyMock.createNiceMock( RowMetaInterface.class );
	  EasyMock.expect( rmi.getFieldNames() ).andReturn( metaNames ).anyTimes();
	  EasyMock.expect( rmi.size() ).andReturn( metaNames.length ).anyTimes();
	  EasyMock.expect( rmi.getValueMeta( EasyMock.anyInt() )).andAnswer( new IAnswer<ValueMetaInterface>(){
		@Override
		public ValueMetaInterface answer() throws Throwable {
			Object[] args = EasyMock.getCurrentArguments();
			int i = Integer.class.cast( args[0] );
			ValueMetaInterface ret = new ValueMetaString( metaNames[i] );
			return ret;
		}} ).anyTimes();
	  EasyMock.replay( rmi );	  
	  return rmi;
  }
  
  private List<MongoDbOutputMeta.MongoField> getMongoFields( String[] names ){
	  List<MongoDbOutputMeta.MongoField> ret = new ArrayList<MongoDbOutputMeta.MongoField>( names.length );
	  for ( int i=0; i<names.length; i++ ){
		  MongoDbOutputMeta.MongoField field = new MongoDbOutputMeta.MongoField();
		  field.m_incomingFieldName = names[i];
		  ret.add( field );
	  }
	  return ret;
  }

  public static void main(String[] args) {
    try {
      MongoDbOutputTest test = new MongoDbOutputTest();
      test.testInsertKettleFieldThatContainsJsonIntoTopLevelRecord();
      test.testInsertKettleFieldThatContainsJsonIntoOneLevelNestedDoc();
      test.testInsertKettleFieldThatContainsJsonIntoTopLevelArray();
      test.testModifierPushComplexObjectWithJsonNestedDoc();
      test.testGetQueryObjectThatContainsJsonNestedDoc();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
