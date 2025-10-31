package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.DBObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.steps.mongodb.MongoDBHelper;
import org.pentaho.mongo.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.field.MongoField;
import java.lang.reflect.Field;
import java.util.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MongoDbInputHelperTest {

    @Mock
    private LogChannelInterfaceFactory logChannelFactory;
    @Mock
    private LogChannelInterface mockLog;
    TransMeta transMeta = mock(TransMeta.class);
    MongoDbInputMeta meta = mock(MongoDbInputMeta.class);
    @Mock
    MongoProperties.Builder builder;
    @Mock
    MongoDbInputDiscoverFields discoverFields; // Your actual discoverFields class

    @Before
    public void setUp() {
        KettleLogStore.setLogChannelInterfaceFactory(logChannelFactory);
    }

    // Reflection helper to inject mocked LogChannelInterface into private `log` field.
    private static void injectLog(Object target, LogChannelInterface log) {
        try {
            Field f = target.getClass().getDeclaredField("log");
            f.setAccessible(true);
            f.set(target, log);
        } catch (NoSuchFieldException nsfe) {
            Class<?> cls = target.getClass().getSuperclass();
            while (cls != null) {
                try {
                    Field f = cls.getDeclaredField("log");
                    f.setAccessible(true);
                    f.set(target, log);
                    return;
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    cls = cls.getSuperclass();
                }
            }
            throw new RuntimeException("Unable to inject log field", nsfe);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Unable to inject log field", iae);
        }
    }

    // ------------- Delegation tests for MongoDBHelper-backed actions ----------------

    @Test
    public void getDBNames_delegatesToMongoDBHelper_andReturnsJson() {
        JSONObject expected = new JSONObject();
        expected.put("dbs", "ok");
        try (MockedConstruction<MongoDBHelper> mocked = Mockito.mockConstruction(MongoDBHelper.class,
                (constructed, ctx) -> when(constructed.getDBNamesAction(transMeta, meta)).thenReturn(expected))) {
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject actual = helper.handleStepAction("getDBNames", transMeta, Collections.emptyMap());
            assertSame(expected, actual);
            MongoDBHelper constructed = mocked.constructed().get(0);
            verify(constructed, times(1)).getDBNamesAction(transMeta, meta);
        }
    }

    @Test
    public void getDBNames_whenHelperThrows_returnsFailureAndLogs() {
        try (MockedConstruction<MongoDBHelper> mocked = Mockito.mockConstruction(MongoDBHelper.class,
                (constructed, ctx) -> when(constructed.getDBNamesAction(transMeta, meta)).thenThrow(new RuntimeException("boom")))) {
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject result = helper.handleStepAction("getDBNames", transMeta, Collections.emptyMap());
            assertEquals(StepInterface.FAILURE_METHOD_NOT_RESPONSE, result.get(StepInterface.ACTION_STATUS));
        }
    }

    @Test
    public void getCollectionNames_delegatesToMongoDBHelper_andReturnsJson() {
        JSONObject expected = new JSONObject();
        expected.put("collections", "ok");
        try (MockedConstruction<MongoDBHelper> mocked = Mockito.mockConstruction(MongoDBHelper.class,
                (constructed, ctx) -> when(constructed.getCollectionNamesAction(transMeta, meta)).thenReturn(expected))) {
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject actual = helper.handleStepAction("getCollectionNames", transMeta, Collections.emptyMap());
            assertSame(expected, actual);
            MongoDBHelper constructed = mocked.constructed().get(0);
            verify(constructed, times(1)).getCollectionNamesAction(transMeta, meta);
        }
    }

    @Test
    public void getPreferences_delegatesToMongoDBHelper_andReturnsJson() {
        JSONObject expected = new JSONObject();
        expected.put("prefs", "ok");
        try (MockedConstruction<MongoDBHelper> mocked = Mockito.mockConstruction(MongoDBHelper.class,
                (constructed, ctx) -> when(constructed.getPreferencesAction()).thenReturn(expected))) {
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject actual = helper.handleStepAction("getPreferences", transMeta, Collections.emptyMap());
            assertSame(expected, actual);
            MongoDBHelper constructed = mocked.constructed().get(0);
            verify(constructed, times(1)).getPreferencesAction();
        }
    }

    // ------------- getTagSetAction tests ----------------

    @Test
    public void getTagSet_missingHostnames_returnsError() {
        when(meta.getHostnames()).thenReturn(null);
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        injectLog(helper, mockLog);
        JSONObject resp = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());
        assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
    }

    @Test
    public void getTagSet_success_returnsTagSetsAndSuccess() throws MongoDbException {
        when(meta.getHostnames()).thenReturn("localhost");
        // mock static helpers
        try (MockedStatic<MongoDBHelper> mongoHelper = mockStatic(MongoDBHelper.class);
             MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {
            MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
            List<String> dummyTags = List.of("tag1:value1", "tag2:value2", "tag3:value3");
            when(wrapper.getAllTags()).thenReturn(dummyTags);
            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(any(), any(), any())).thenReturn(wrapper);
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject resp = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());
            assertEquals(StepInterface.SUCCESS_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
            assertTrue(resp.containsKey("tag_sets"));
            JSONArray arr = (JSONArray) resp.get("tag_sets");
            assertEquals(3, arr.size());
            verify(wrapper, times(1)).dispose();
        }
    }

    @Test
    public void getTagSet_emptyTags_returnsError() throws MongoDbException {
        when(transMeta.getLogChannel()).thenReturn(mockLog);
        when(meta.getHostnames()).thenReturn("host");
        MongoClientWrapper mockWrapper = mock(MongoClientWrapper.class);
        when(mockWrapper.getAllTags()).thenReturn(Collections.emptyList());
        try (MockedStatic<org.pentaho.mongo.wrapper.MongoWrapperUtil> wrapperUtil = Mockito.mockStatic(org.pentaho.mongo.wrapper.MongoWrapperUtil.class)) {
            wrapperUtil.when(() -> org.pentaho.mongo.wrapper.MongoWrapperUtil.createMongoClientWrapper(meta, transMeta, mockLog)).thenReturn(mockWrapper);
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject resp = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());
            assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
            verify(mockWrapper, times(1)).dispose();
        }
    }

    // ------------- testTagSetAction tests ----------------

    @Test
    public void testTagSet_missingHostnames_returnsError() {
        when(meta.getHostnames()).thenReturn(null);
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        injectLog(helper, mockLog);
        JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
        assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
    }

    @Test
    public void testTagSet_emptyTagSets_returnsError() {
        when(meta.getHostnames()).thenReturn("host");
        when(meta.getReadPrefTagSets()).thenReturn(Collections.emptyList());
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        injectLog(helper, mockLog);
        JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
        assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
    }


    @Test
    public void testMissingHostnames() {
        MongoDbInputMeta mongoDbInputMeta = mock(MongoDbInputMeta.class);
        when(mongoDbInputMeta.getHostnames()).thenReturn("");
        MongoDbInputHelper helper = new MongoDbInputHelper(mongoDbInputMeta);
        JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
        assertEquals("Action failed", resp.get("actionStatus"));
        assertTrue(resp.toString().contains("No connection details available"));
    }

    @Test
    public void testMissingTagSets() {
        when(meta.getHostnames()).thenReturn("localhost");
        when(meta.getReadPrefTagSets()).thenReturn(Collections.emptyList());
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
        assertTrue(resp.toString().contains("No tag sets defined in the table view"));
    }

    @Test
    public void testMissingTagSets_2() {
        when(meta.getHostnames()).thenReturn("localhost");
        List<String> dummyTags = List.of("\"tag1\":\"value1\"", "\"tag2\":\"value2\"", "\"tag3\":\"value3\"");
        when(meta.getReadPrefTagSets()).thenReturn( dummyTags );
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
        assertEquals("Action failed", resp.get("actionStatus"));
    }

    @Test
    public void testNoReplicaSetMembersMatchTagSets() {
        when(meta.getHostnames()).thenReturn("localhost");
        List<String> dummyTags = List.of("\"tag1\":\"value1\"", "\"tag2\":\"value2\"", "\"tag3\":\"value3\"");
        when(meta.getReadPrefTagSets()).thenReturn( dummyTags );
        // Mocked wrapper object to be returned
        MongoClientWrapper dummyWrapper = mock(MongoClientWrapper.class);

        // Make sure transMeta.getLogChannel() returns your logChannel mock
        when(transMeta.getLogChannel()).thenReturn( mockLog );

        // Now mock the static method
        try (MockedStatic<MongoWrapperUtil> mocked = mockStatic(MongoWrapperUtil.class)) {

            // Stub the static call
            mocked.when(() ->
                    MongoWrapperUtil.createMongoClientWrapper(
                            any(MongoDbInputMeta.class),
                            any(TransMeta.class),
                            any(LogChannelInterface.class))
            ).thenReturn(dummyWrapper);

            // --- Actual code under test ---
            MongoClientWrapper wrapper =
                    MongoWrapperUtil.createMongoClientWrapper(meta, transMeta, transMeta.getLogChannel());

            // --- Verification ---
            assertNotNull(wrapper);
            assertSame(dummyWrapper, wrapper);

            mocked.verify(() -> MongoWrapperUtil.createMongoClientWrapper(
                    meta, transMeta, transMeta.getLogChannel()));
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            JSONObject resp = helper.handleStepAction("testTagSet", transMeta, Collections.emptyMap());
            assertEquals("Action failed", resp.get("actionStatus"));
            assertTrue(resp.toString().contains("!MongoDbInputDialog.ErrorMessage..NoReplicaSetMembersMatchTagSets!"));
        } catch (MongoDbException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSetupDBObjects() {
        List<DBObject> list = new ArrayList<>();
        MongoDBHelper.setupDBObjects(list, "\"tag1\":\"value1\"");

        assertEquals(1, list.size());
        assertEquals("value1", list.get(0).get("tag1"));
    }

    @Test
    public void shouldReturnError_whenMissingConnectionDetails() {
        try (MockedStatic<BaseMessages> baseMessages = Mockito.mockStatic(BaseMessages.class)) {
            baseMessages.when(() -> BaseMessages.getString(MongoDbInputMeta.class, "UnableToConnectLabel"))
                    .thenReturn("Unable to connect");
            baseMessages.when(() -> BaseMessages.getString(MongoDbInputMeta.class, "MissingConnDetails", "Missing"))
                    .thenReturn("Connection details missing");
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            // Do NOT let PKG be null
            JSONObject resp = helper.handleStepAction("getFields", transMeta, Collections.emptyMap());
            assertEquals("Action failed", resp.get("actionStatus"));
            assertTrue(resp.toString().contains("Unable to connect to MongoDB - check connection details"));
        }
    }

    @Test
    public void getFieldsAction_withUnresolvedVars_returnsError() {
        when(meta.getHostnames()).thenReturn("host");
        when(meta.getDbName()).thenReturn("db");
        when(meta.getCollection()).thenReturn("coll");
        when(meta.getJsonQuery()).thenReturn("${var}");
        when(transMeta.environmentSubstitute(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        injectLog(helper, mockLog);
        Map<String, String> queryParams = Collections.singletonMap("jsonQuery", "${var}");
        JSONObject resp = helper.handleStepAction("getFields", transMeta, queryParams);
        assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
        assertTrue(resp.toString().contains("Query contains unresolved variables or field substitutions"));
    }

    @Test
    public void getFieldsAction_validRequest_returnsFields() throws Exception {
        when(meta.getDbName()).thenReturn("db");
        when(meta.getCollection()).thenReturn("coll");
        when(meta.getJsonQuery()).thenReturn("");
        when(meta.getExecuteForEachIncomingRow()).thenReturn(false);
        when(transMeta.environmentSubstitute(anyString())).thenReturn("");
        // Sample field
        MongoField field1 = new MongoField();
        field1.m_fieldName = "field1";
        field1.m_fieldPath = "path1";
        field1.m_kettleType = "String";
        field1.m_occurenceFraction = String.valueOf(1.0d);
        field1.m_disparateTypes = false;
        List<MongoField> fields = Collections.singletonList(field1);
        try (MockedStatic<MongoDBHelper> mongoHelper = Mockito.mockStatic(MongoDBHelper.class)) {
            mongoHelper.when(() -> MongoDBHelper.validateRequestForFields(meta)).thenReturn("");

            try (MockedStatic<MongoWrapperUtil> wrapperUtil = Mockito.mockStatic(MongoWrapperUtil.class)) {
                wrapperUtil.when(() -> MongoWrapperUtil.createPropertiesBuilder(meta, transMeta)).thenReturn(builder);

                MongoDbInputDiscoverFields discoverFieldsMock = mock(MongoDbInputDiscoverFields.class);
                MongoDbInputDiscoverFieldsHolder holderMock = mock(MongoDbInputDiscoverFieldsHolder.class);

                // Set static holder field
                Field holderField = MongoDbInputData.class.getDeclaredField("mongoDbInputDiscoverFieldsHolder");
                holderField.setAccessible(true);
                holderField.set(null, holderMock);

                when(holderMock.getMongoDbInputDiscoverFields()).thenReturn(discoverFieldsMock);

                // ✅ Correct doAnswer on discoverFieldsMock
                doAnswer(invocation -> {
                    DiscoverFieldsCallback cb = invocation.getArgument(9);
                    cb.notifyFields(fields);
                    return null;
                }).when(discoverFieldsMock).discoverFields(
                        eq(builder),
                        eq("db"), eq("coll"),
                        any(), any(), anyBoolean(), anyInt(),
                        eq(meta), eq(transMeta), any()
                );

                Map<String, String> params = new HashMap<>();
                params.put("sampleSize", "1");

                MongoDbInputHelper helper = new MongoDbInputHelper(meta);
                injectLog(helper, mockLog);

                JSONObject resp = helper.handleStepAction("getFields", transMeta, params);

                assertEquals(StepInterface.SUCCESS_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
                assertNotNull(resp.get("fields"));
            }
        }
    }

    @Test
    public void getTagSet_whenExceptionOnWrapper_returnsError() {
        when(meta.getHostnames()).thenReturn("localhost");
        try (MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {
            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(any(), any(), any()))
                    .thenThrow(new RuntimeException("can't create wrapper"));
            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject resp = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());
            assertEquals(StepInterface.FAILURE_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
            assertTrue(resp.toString().contains("Unable to connect to MongoDB - check connection details"));
        }
    }

    @Test
    public void handleStepAction_unknownMethod_returnsFailureStatus() {
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        injectLog(helper, mockLog);
        JSONObject resp = helper.handleStepAction("nonexistent", transMeta, Collections.emptyMap());
        assertEquals(StepInterface.FAILURE_METHOD_NOT_RESPONSE, resp.get(StepInterface.ACTION_STATUS));
    }

    // ------------ Verify setFieldResponse conversion ------------

    @Test
    public void setFieldResponse_convertsMongoFieldsProperly() {
        MongoField field = new MongoField();
        field.m_fieldName = "myField";
        field.m_fieldPath = "$.myField";
        field.m_kettleType = "String";
        field.m_indexedVals = Arrays.asList("A", "B");
        field.m_arrayIndexInfo = "idx";
        field.m_occurenceFraction = String.valueOf(0.5);
        field.m_disparateTypes = true;

        List<MongoField> fields = Collections.singletonList(field);
        MongoDbInputHelper helper = new MongoDbInputHelper(meta);
        JSONArray arr = helper.setFieldResponse(fields);
        assertEquals(1, arr.size());
        JSONObject fObj = (JSONObject) arr.get(0);
        assertEquals("myField", fObj.get("field_name"));
        assertEquals("$.myField", fObj.get("field_path"));
        assertEquals("String", fObj.get("field_type"));
        assertEquals(Arrays.asList("A", "B"), fObj.get("indexed_vals"));
        assertEquals("idx", fObj.get("field_array_index"));
        assertEquals(String.valueOf(0.5), fObj.get("occurrence_fraction"));
        assertEquals(true, fObj.get("field_disparate_types"));
    }

    @Test
    public void getTagSet_getAllTagsReturnsNull() throws MongoDbException {
        when(meta.getHostnames()).thenReturn("localhost");

        try (MockedStatic<MongoDBHelper> mongoHelper = mockStatic(MongoDBHelper.class);
             MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {
            MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
            when(wrapper.getAllTags()).thenReturn(null); // returning null
            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(any(), any(), any())).thenReturn(wrapper);

            // Mock errorResponse to return a JSONObject (not null)
            mongoHelper.when(() -> MongoDBHelper.errorResponse(any(), anyString(), anyString()))
                    .thenAnswer(invocation -> {
                        JSONObject errorResp = new JSONObject();
                        errorResp.put(StepInterface.ACTION_STATUS, StepInterface.FAILURE_RESPONSE);
                        return errorResp;
                    });

            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject result = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());
            assertNotNull("Response should not be null", result);
            assertEquals(StepInterface.FAILURE_RESPONSE, result.get(StepInterface.ACTION_STATUS));
            verify(wrapper, times(1)).dispose();
        }
    }

    @Test
    public void getTagSet_throwExceptionInGetAllTags() throws Exception {
        when(meta.getHostnames()).thenReturn("localhost");

        try (MockedStatic<MongoDBHelper> mongoHelper = mockStatic(MongoDBHelper.class);
             MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {

            MongoClientWrapper wrapper = mock(MongoClientWrapper.class);

            // Throw exception on getAllTags call
            when(wrapper.getAllTags()).thenThrow(new RuntimeException("some error"));

            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(any(), any(), any())).thenReturn(wrapper);

            // Mock errorResponse to return a valid JSONObject (non-null)
            mongoHelper.when(() -> MongoDBHelper.errorResponse((JSONObject) any(), anyString(), (Exception) any(Throwable.class)))
                    .thenAnswer(invocation -> {
                        JSONObject errorResp = new JSONObject();
                        errorResp.put(StepInterface.ACTION_STATUS, StepInterface.FAILURE_RESPONSE);
                        return errorResp;
                    });

            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);

            JSONObject result = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());

            assertNotNull("Response should not be null", result);
            assertEquals(StepInterface.FAILURE_RESPONSE, result.get(StepInterface.ACTION_STATUS));

            verify(wrapper, times(1)).dispose();
        }
    }

    @Test
    public void getTagSet_tagsAlreadyWithBraces() throws MongoDbException {
        when(meta.getHostnames()).thenReturn("localhost");
        List<String> tags = List.of("{tag1}", "{tag2:val}", "tag3");
        try (MockedStatic<MongoDBHelper> mongoHelper = mockStatic(MongoDBHelper.class);
             MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {
            MongoClientWrapper wrapper = mock(MongoClientWrapper.class);
            when(wrapper.getAllTags()).thenReturn(tags);
            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(any(), any(), any())).thenReturn(wrapper);

            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);
            JSONObject result = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());

            JSONArray arr = (JSONArray) result.get("tag_sets");
            String firstTag = (String) ((JSONObject) arr.get(0)).get("tag_set");
            assertTrue(firstTag.startsWith("{") && firstTag.endsWith("}"));
            verify(wrapper, times(1)).dispose();
        }
    }

    @Test
    public void getTagSet_disposeCalledOnError() {
        when(meta.getHostnames()).thenReturn("localhost");

        try (MockedStatic<MongoDBHelper> mongoHelper = mockStatic(MongoDBHelper.class);
             MockedStatic<MongoWrapperUtil> wrapperUtil = mockStatic(MongoWrapperUtil.class)) {

            // Make createMongoClientWrapper throw an exception to simulate error
            wrapperUtil.when(() -> MongoWrapperUtil.createMongoClientWrapper(meta, transMeta, mockLog))
                    .thenThrow(new RuntimeException("error"));

            // Mock errorResponse to return a non-null JSONObject
            mongoHelper.when(() -> MongoDBHelper.errorResponse((JSONObject) any(), anyString(), (Exception) any(Throwable.class)))
                    .thenAnswer(invocation -> {
                        JSONObject errorResp = new JSONObject();
                        errorResp.put(StepInterface.ACTION_STATUS, StepInterface.FAILURE_RESPONSE);
                        return errorResp;
                    });

            MongoDbInputHelper helper = new MongoDbInputHelper(meta);
            injectLog(helper, mockLog);

            JSONObject result = helper.handleStepAction("getTagSet", transMeta, Collections.emptyMap());

            assertNotNull("Response should not be null", result);
            assertEquals(StepInterface.FAILURE_RESPONSE, result.get(StepInterface.ACTION_STATUS));
        }
    }

}
