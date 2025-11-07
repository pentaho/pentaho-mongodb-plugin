package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.DBObject;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.row.*;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.TransMeta;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MongoDbOutputUtilHelper
 *
 * Assumptions:
 * - MongoDbOutputMeta.MongoField is instantiable and has public fields accessed by the helper
 * - Mockito supports static mocking (mockStatic)
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDbOutputUtilHelperTest {

  private MongoDbOutputUtilHelper helper;

  @Mock
  private TransMeta transMeta;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    helper = new MongoDbOutputUtilHelper();
  }

  @After
  public void tearDown() {
    // nothing
  }

  //@Test
  public void testPrettyPrintDocStructure_null_and_empty() {
    assertNull(helper.prettyPrintDocStructure(null));
    assertEquals("", helper.prettyPrintDocStructure(""));
  }

  @Test
  public void testPrettyPrintDocStructure_simpleObject() {
    // simple object string
    String formatted = helper.prettyPrintDocStructure("{a:1,b:2}");
    // Expect braces and properties on separate lines (basic structural check)
    assertTrue(formatted.contains("{"));
    assertTrue(formatted.contains("}"));
    assertTrue(formatted.contains("a:1") || formatted.contains("a: 1"));
  }

  @Test
  public void testPrettyPrintDocStructure_nestedStructuresAndArrays() {
    String src = "{a:{b:1,c:[2,3]},d:4}";
    String formatted = helper.prettyPrintDocStructure(src);
    // basic checks to ensure nesting got indented and arrays processed
    assertTrue(formatted.contains("{"));
    assertTrue(formatted.contains("["));
    assertTrue(formatted.split("\n").length > 1);
  }

  @Test
  public void test_formatSource_commaTrimming() {
    // Private method formatSource is used by prettyPrintDocStructure, but test trimming effect indirectly:
    String src = "{ a : 1 , b : 2 }";
    String formatted = helper.prettyPrintDocStructure(src);
    assertTrue(formatted.length() > 0);
  }

    @Test
    public void test_getMongoDataTypeDocument_variousTypes() throws Exception {
        // Mock static ValueMetaFactory completely
        try (MockedStatic<ValueMetaFactory> factoryMock = mockStatic(ValueMetaFactory.class)) {
            RowMeta rowMeta = new RowMeta();

            // Mock ValueMetaInterface for all supported types
            ValueMetaInterface vmString = mock(ValueMetaInterface.class);
            when(vmString.getType()).thenReturn(ValueMetaInterface.TYPE_STRING);
            when(vmString.getName()).thenReturn("strField");

            ValueMetaInterface vmInt = mock(ValueMetaInterface.class);
            when(vmInt.getType()).thenReturn(ValueMetaInterface.TYPE_INTEGER);
            when(vmInt.getName()).thenReturn("intField");

            ValueMetaInterface vmNumber = mock(ValueMetaInterface.class);
            when(vmNumber.getType()).thenReturn(ValueMetaInterface.TYPE_NUMBER);
            when(vmNumber.getName()).thenReturn("numField");

            ValueMetaInterface vmBool = mock(ValueMetaInterface.class);
            when(vmBool.getType()).thenReturn(ValueMetaInterface.TYPE_BOOLEAN);
            when(vmBool.getName()).thenReturn("boolField");

            ValueMetaInterface vmDate = mock(ValueMetaInterface.class);
            when(vmDate.getType()).thenReturn(ValueMetaInterface.TYPE_DATE);
            when(vmDate.getName()).thenReturn("dateField");

            ValueMetaInterface vmBinary = mock(ValueMetaInterface.class);
            when(vmBinary.getType()).thenReturn(ValueMetaInterface.TYPE_BINARY);
            when(vmBinary.getName()).thenReturn("binField");

            ValueMetaInterface vmUnknown = mock(ValueMetaInterface.class);
            when(vmUnknown.getType()).thenReturn(999);
            when(vmUnknown.getName()).thenReturn("unkField");

            // Mock ValueMetaFactory responses by name or type
            factoryMock.when(() -> ValueMetaFactory.createValueMeta(anyString(), anyInt())).thenAnswer(invocation -> {
                String name = invocation.getArgument(0);
                if ("strField".equals(name)) return vmString;
                if ("intField".equals(name)) return vmInt;
                if ("numField".equals(name)) return vmNumber;
                if ("boolField".equals(name)) return vmBool;
                if ("dateField".equals(name)) return vmDate;
                if ("binField".equals(name)) return vmBinary;
                if ("unkField".equals(name)) return vmUnknown;
                return vmString;
            });

            // RowMeta will simply hold our mock metas
            rowMeta.addValueMeta(vmString);
            rowMeta.addValueMeta(vmInt);
            rowMeta.addValueMeta(vmNumber);
            rowMeta.addValueMeta(vmBool);
            rowMeta.addValueMeta(vmDate);
            rowMeta.addValueMeta(vmBinary);
            rowMeta.addValueMeta(vmUnknown);

           // Access the private method
            Method m = MongoDbOutputUtilHelper.class.getDeclaredMethod(
                    "getMongoDataTypeDocument", MongoDbOutputMeta.MongoField.class, RowMetaInterface.class);
            m.setAccessible(true);

            MongoDbOutputMeta.MongoField fld;

            // ---- STRING ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "strField";
            fld.m_JSON = false;
            assertEquals("<string val>", m.invoke(helper, fld, rowMeta));

            // ---- JSON true + no path ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "strField";
            fld.m_JSON = true;
            fld.m_useIncomingFieldNameAsMongoFieldName = false;
            fld.environUpdateMongoDocPath = "";
            String resJson = (String) m.invoke(helper, fld, rowMeta);
            assertTrue(resJson.contains("IncomingJSONDoc"));

            // ---- INTEGER ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "intField";
            assertEquals("<integer val>", m.invoke(helper, fld, rowMeta));

            // ---- NUMBER ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "numField";
            assertEquals("<number val>", m.invoke(helper, fld, rowMeta));

            // ---- BOOLEAN ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "boolField";
            assertEquals("<bool val>", m.invoke(helper, fld, rowMeta));

            // ---- DATE ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "dateField";
            assertEquals("<date val>", m.invoke(helper, fld, rowMeta));

            // ---- BINARY ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "binField";
            assertEquals("<binary val>", m.invoke(helper, fld, rowMeta));

            // ---- UNSUPPORTED ----
            fld = new MongoDbOutputMeta.MongoField();
            fld.environUpdatedFieldName = "unkField";
            assertEquals("<unsupported value type>", m.invoke(helper, fld, rowMeta));
        }
    }


    @Test
  public void test_getDisplayStringForUpdate_privateMethod() throws Exception {
    // Create fake DBObjects with predictable toString
    DBObject query = mock(DBObject.class);
    DBObject modifier = mock(DBObject.class);
    when(query.toString()).thenReturn("{q:1}");
    when(modifier.toString()).thenReturn("{m:2}");

    // stub BaseMessages to return the key (used by getString)
    try (var baseMsg = mockStatic(org.pentaho.di.i18n.BaseMessages.class)) {
      baseMsg.when(() -> org.pentaho.di.i18n.BaseMessages.getString(any(Class.class), anyString()))
          .thenAnswer(invocation -> invocation.getArgument(1));

      Method m = MongoDbOutputUtilHelper.class.getDeclaredMethod("getDisplayStringForUpdate", DBObject.class, DBObject.class);
      m.setAccessible(true);
      String out = (String) m.invoke(helper, query, modifier);

      assertTrue(out.contains("MongoDbOutputDialog.PreviewModifierUpdate.Heading1"));
      assertTrue(out.contains("q:1"));
      assertTrue(out.contains("m:2"));
    }
  }

    @Test
    public void test_previewDocStructure_updateSelection_false_and_true() throws Exception {
        // Prepare one MongoField instance
        MongoDbOutputMeta.MongoField fld = new MongoDbOutputMeta.MongoField();
        fld.environUpdatedFieldName = "field1";
        fld.m_JSON = false;
        fld.m_useIncomingFieldNameAsMongoFieldName = false;
        fld.environUpdateMongoDocPath = "";
        fld.m_modifierOperationApplyPolicy = "";

        List<MongoDbOutputMeta.MongoField> fields = Collections.singletonList(fld);

        RowMeta prev = new RowMeta();
        when(transMeta.getPrevStepFields(anyString())).thenReturn(prev);

        // ✅ Fix #1: Mock variable-related calls to avoid NPE
        when(transMeta.listVariables()).thenReturn(new String[0]);

        // ✅ Fix #2: Mock ValueMetaFactory so it won’t throw plugin exception
        try (MockedStatic<ValueMetaFactory> vmFactory = mockStatic(ValueMetaFactory.class);
             MockedStatic<MongoDbOutputData> staticMock = mockStatic(MongoDbOutputData.class);
             MockedStatic<org.pentaho.di.i18n.BaseMessages> baseMsg =
                     mockStatic(org.pentaho.di.i18n.BaseMessages.class)) {

            ValueMetaInterface fakeMeta = mock(ValueMetaInterface.class);
            when(fakeMeta.getName()).thenReturn("field1");
            vmFactory.when(() -> ValueMetaFactory.createValueMeta(anyString(), anyInt())).thenReturn(fakeMeta);
            vmFactory.when(() -> ValueMetaFactory.createValueMeta(anyInt())).thenReturn(fakeMeta);

            // Mock MongoDbOutputData static methods
            staticMock.when(() -> MongoDbOutputData.scanForInsertTopLevelJSONDoc(anyList())).thenReturn(false);
            MongoDbOutputData.MongoTopLevel topLevel = mock(MongoDbOutputData.MongoTopLevel.class);
            staticMock.when(() -> MongoDbOutputData.checkTopLevelConsistency(anyList(), any(VariableSpace.class)))
                    .thenReturn(topLevel);

            DBObject doc = mock(DBObject.class);
            when(doc.toString()).thenReturn("{a:1,b:[2,3]}");
            staticMock.when(() -> MongoDbOutputData.kettleRowToMongo(anyList(), any(RowMetaInterface.class),
                            any(Object[].class), any(MongoDbOutputData.MongoTopLevel.class), anyBoolean()))
                    .thenReturn(doc);

            baseMsg.when(() -> org.pentaho.di.i18n.BaseMessages.getString(any(Class.class), anyString()))
                    .thenAnswer(inv -> inv.getArgument(1));

            // Case 1: updateSelection = false
            Map<String, String> res = helper.previewDocStructure(transMeta, "step1", fields, false);
            assertNotNull(res);
            assertEquals("MongoDbOutputDialog.PreviewDocStructure.Title", res.get("windowTitle"));
            assertTrue(res.get("toDisplay").contains("a:1"));

            // Case 2: updateSelection = true
            DBObject qObj = mock(DBObject.class);
            when(qObj.toString()).thenReturn("{q:1}");
            staticMock.when(() -> MongoDbOutputData.getQueryObject(anyList(), any(RowMetaInterface.class),
                            any(Object[].class), any(VariableSpace.class), any(MongoDbOutputData.MongoTopLevel.class)))
                    .thenReturn(qObj);

            try (var construction = Mockito.mockConstruction(MongoDbOutputData.class,
                    (mock, context) -> {
                        when(mock.getModifierUpdateObject(anyList(), any(RowMetaInterface.class), any(Object[].class),
                                any(VariableSpace.class), any(MongoDbOutputData.MongoTopLevel.class)))
                                .thenReturn(mock(DBObject.class, invocation -> "{m:2}"));
                    })) {

                Map<String, String> res2 = helper.previewDocStructure(transMeta, "step1", fields, true);
                assertNotNull(res2);
                assertEquals("MongoDbOutputDialog.PreviewModifierUpdate.Title", res2.get("windowTitle"));
                assertTrue(res2.get("toDisplay").contains("MongoDbOutputDialog.PreviewModifierUpdate.Heading1"));
            }
        }
    }

}
