package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.BasicDBObject;
import junit.framework.TestCase;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mongodbinput.BaseMongoDbStepTest;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import java.util.*;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.pentaho.di.trans.steps.mongodb.MongoDBHelper.ERROR_MESSAGE;

@RunWith( MockitoJUnitRunner.class )
public class MongoDbOutputHelperTest extends BaseMongoDbStepTest {
  private static final Class<?> PKG = MongoDbOutputMeta.class;
  @Mock
  private MongoDbOutputData stepDataInterace;
  @Mock private MongoDbOutputMeta stepMetaInterface;

  private MongoDbOutput dbOutput;
  RowMeta rowMeta = new RowMeta();
  MongoDbOutputMeta meta = mock(MongoDbOutputMeta.class);
  TransMeta transMeta = mock(TransMeta.class);
  private List<MongoDbOutputMeta.MongoField> mongoFields = new ArrayList<MongoDbOutputMeta.MongoField>();
  MongoDbOutputHelper helper = new MongoDbOutputHelper(meta);
  @Mock protected MongoCollectionWrapper mongoCollectionWrapper;

  @BeforeClass
    public static void beforeClass() throws Exception {
        PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
        PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
        PluginRegistry.init();
        String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
        Encr.init( passwordEncoderPluginID );
    }

    @Before
    public void setUp() {
        KettleLogStore.setLogChannelInterfaceFactory(logChannelFactory);
        when( meta.getConnectionString() ).thenReturn( "mongodb://validUser:password@localhost:2730/validDB" );
        when( meta.getHostnames() ).thenReturn( "localhost" );
    }

    @Test
    public void testConnection_withValidCredentials() {
        JSONObject jsonObject = helper.stepAction( "testConnection", transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "isValidConnection" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
        assertTrue( (boolean) jsonObject.get( "isValidConnection" ) );
    }

    @Test
    public void testConnection_withInvalidCredentials() throws MongoDbException {
        when( mongoClientWrapperFactory
                .createMongoClientWrapper( Mockito.<MongoProperties>any(), Mockito.<MongoUtilLogger>any() ) )
                .thenThrow( new MongoDbException() );
        JSONObject jsonObject = helper.stepAction( "testConnection", transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "isValidConnection" ) );
        TestCase.assertFalse( (boolean) jsonObject.get( "isValidConnection" ) );
    }

    @Test
    public void testConnection_withEmptyConnectionString() {
        when( meta.getConnectionString() ).thenReturn( "" );
        JSONObject jsonObject = helper.stepAction( "testConnection", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        assertNotNull( jsonObject.get( "errorMessage" ) );
    }

    @Test
    public void getDBNamesTest() throws MongoDbException {
        when( mongoClientWrapper.getDatabaseNames() ).thenReturn( Collections.singletonList( "mockDB" ) );
        JSONObject jsonObject = helper.stepAction( "getDBNames", transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "dbNames" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

    @Test
    public void getDBNamesTest_throwsException() throws MongoDbException {
        when( mongoClientWrapper.getDatabaseNames() ).thenThrow( new MongoDbException( "error" ) );
        JSONObject jsonObject = helper.stepAction( "getDBNames", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        assertNotNull( jsonObject.get( "errorMessage" ) );
    }

    @Test
    public void getDBNamesTest_withHostNameAndConnStringMissing() {
        when( meta.getHostnames() ).thenReturn( "" );
        when( meta.getConnectionString() ).thenReturn( "" );
        JSONObject jsonObject = helper.stepAction( "getDBNames", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        TestCase.assertEquals( jsonObject.get( "errorMessage" ), "Some connection/configuration details are missing: Hostname" );
    }

    @Test
    public void getCollectionNamesTest() {
        setupReturns();
        JSONObject jsonObject = helper.stepAction( "getCollectionNames", transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "collectionNames" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

    @Test
    public void getCollectionNamesTest_throwsException() {
        when( meta.getHostnames() ).thenReturn( "localhost" );
        JSONObject jsonObject = helper.stepAction( "getCollectionNames", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        assertNotNull( jsonObject.get( "errorMessage" ) );
    }

    @Test
    public void getCollectionNamesTest_WithHostNameAndConnStringMissing() {
        when( meta.getHostnames() ).thenReturn( "" );
        when( meta.getConnectionString() ).thenReturn( "" );
        JSONObject jsonObject = helper.stepAction( "getCollectionNames", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        TestCase.assertEquals( jsonObject.get( "errorMessage" ), "Some connection/configuration details are missing: Hostname" );
    }

    @Test
    public void getCollectionNamesTest_WithDBNameMissing() {
        when( meta.getDbName() ).thenReturn( "" );
        JSONObject jsonObject = helper.stepAction( "getCollectionNames", transMeta , new HashMap<>() );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        assertNotNull( jsonObject.get( ERROR_MESSAGE ) );
    }

    @Test
    public void getPreferencesTest() {
        JSONObject jsonObject = helper.stepAction( "getPreferences", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        List<String> preferences = (List<String>) jsonObject.get( "preferences" );
        assertNotNull( preferences );
        assertTrue( preferences.size() > 0 );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

    @Test
    public void writeConcernsTest() throws MongoDbException {
        when( transMeta.environmentSubstitute( any( String.class ) ) ).thenReturn( "testHost" );
        when( mongoClientWrapper.getLastErrorModes() ).thenReturn( Collections.singletonList( "testMode" ) );
        JSONObject jsonObject = helper.stepAction( "writeConcerns", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        List<String> writeConcerns = (List<String>) jsonObject.get( "writeConcerns" );
        assertNotNull( writeConcerns );
        assertTrue( writeConcerns.size() > 0 );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

    @Test
    public void writeConcernsTest_withConnectionDetailsMissing() {
        missingHostTest( "writeConcerns" );
    }

    @Test
    public void writeConcernsTest_throwsExceptionOnErrorModes() throws MongoDbException {
        when( transMeta.environmentSubstitute( any( String.class ) ) ).thenReturn( "testHost" );
        when( mongoClientWrapper.getLastErrorModes() ).thenThrow( new MongoDbException() );
        JSONObject jsonObject = helper.stepAction( "writeConcerns", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
    }

    @Test
    public void getDocumentFieldsTest() throws KettleStepException {
        setupReturns();
        StepMeta stepMeta = mock( StepMeta.class );
        when( stepMeta.getName() ).thenReturn( "mongoDbOutput" );
        when( meta.getParentStepMeta() ).thenReturn( stepMeta );
        ValueMetaString valueMetaString = new ValueMetaString();
        valueMetaString.setName( "name" );
        rowMeta.setValueMetaList( Collections.singletonList( valueMetaString ) );
        when( transMeta.getPrevStepFields( "mongoDbOutput" ) ).thenReturn( rowMeta );
        JSONObject jsonObject = helper.stepAction( "getDocumentFields", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        List<String> fields = (List<String>) jsonObject.get( "fields" );
        assertNotNull( fields );
        assertTrue( fields.size() > 0 );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

   @Test
    public void getDocumentFieldsTest_withMissingHostName() {
        missingHostTest( "getDocumentFields" );
    }

    @Test
    public void getDocumentFieldsTest_whenThrowsException() {
        setupReturns();
        StepMeta stepMeta = mock( StepMeta.class );
        when( meta.getParentStepMeta() ).thenReturn( stepMeta );
        JSONObject jsonObject = helper.stepAction( "getDocumentFields", transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "errorMessage" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
    }

    @Test
    public void previewDocumentTest() throws KettleStepException {
        setupReturns();
        StepMeta stepMeta = mock( StepMeta.class );
        when( stepMeta.getName() ).thenReturn( "mongoDbOutput" );
        when( meta.getParentStepMeta() ).thenReturn( stepMeta );
        ValueMetaString valueMetaString = new ValueMetaString( "name" );
        rowMeta.setValueMetaList( Collections.singletonList( valueMetaString ) );
        when( transMeta.getPrevStepFields( "mongoDbOutput" ) ).thenReturn( rowMeta );
        try (MockedStatic<MongoDbOutputData> mongoDbOutputDataMockedStatic = mockStatic( MongoDbOutputData.class );
             MockedConstruction<Variables> ignored = mockConstruction( Variables.class, (mock, context) -> {
                 doNothing().when( mock ).initializeVariablesFrom( transMeta );
             }  ) ) {
            mongoDbOutputDataMockedStatic.when( () -> MongoDbOutputData.kettleRowToMongo( any(), any(), any(), any(), anyBoolean() ) )
                    .thenReturn( new BasicDBObject( Map.of( "name", "value" ) ) );
            JSONObject jsonObject = helper.stepAction( "previewDocumentStructure", transMeta , new HashMap<>() );
            assertNotNull( jsonObject );
            String windowTitle = (String) jsonObject.get( "windowTitle" );
            String displayDetails = (String) jsonObject.get( "toDisplay" );
            assertNotNull( windowTitle );
            assertNotNull( displayDetails );
            assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
        }
    }

    @Test
    public void previewDocument_whenThrowsException() {
        setupReturns();
        StepMeta stepMeta = mock( StepMeta.class );
        when( stepMeta.getName() ).thenReturn( "mongoDbOutput" );
        when( meta.getParentStepMeta() ).thenReturn( stepMeta );
        try ( MockedStatic<MongoDbOutputData> mongoDbOutputDataMockedStatic = mockStatic( MongoDbOutputData.class ) ) {
            mongoDbOutputDataMockedStatic.when( () -> MongoDbOutputData.scanForInsertTopLevelJSONDoc( any() ) )
                    .thenThrow( new KettleStepException( "error" ) );
            JSONObject jsonObject = helper.stepAction( "previewDocumentStructure", transMeta , new HashMap<>() );
            assertNotNull( jsonObject.get( "errorMessage" ) );
            assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
        }
    }

    @Test
    public void previewDocumentTest_withMissingConnectionDetails() {
        missingHostTest( "previewDocumentStructure" );
    }

    @Test
    public void showIndexesTest() throws MongoDbException {
        setupReturns();
        when( transMeta.environmentSubstitute( anyString() ) ).thenReturn( "fieldName" );
        when( mongoClientWrapper.getIndexInfo( "fieldName", "fieldName" ) ).thenReturn( Collections.singletonList( "indexes" ) );
        JSONObject jsonObject = helper.stepAction( "showIndexes", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        String indexes = (String) jsonObject.get( "indexes" );
        assertNotNull( indexes );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.SUCCESS_RESPONSE ) );
    }

    @Test
    public void showIndexesTest_throwsMongoDbException() throws MongoDbException {
        setupReturns();
        when( transMeta.environmentSubstitute( anyString() ) ).thenReturn( "fieldName" );
        when( mongoClientWrapper.getIndexInfo( "fieldName", "fieldName" ) ).thenThrow( new MongoDbException( "error" ) );
        JSONObject jsonObject = helper.stepAction( "showIndexes", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        assertNotNull( jsonObject.get( "errorMessage" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
    }

    @Test
    public void showIndexesTest_withMissingConnectionDetails() {
        missingHostTest( "showIndexes" );
    }

    @Test
    public void doActionTest_withInvalidFieldName() {
        JSONObject jsonObject = helper.stepAction( "invalidFieldName", transMeta , new HashMap<>() );
        assertNotNull( jsonObject );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_METHOD_NOT_RESPONSE ) );
    }

    private void missingHostTest( String fieldName ) {
        when( meta.getHostnames() ).thenReturn( "" );
        JSONObject jsonObject = helper.stepAction( fieldName, transMeta , new HashMap<>() );
        assertNotNull( jsonObject.get( "errorMessage" ) );
        assertThat( jsonObject.get( StepInterface.ACTION_STATUS ), equalTo( StepInterface.FAILURE_RESPONSE ) );
    }

    private void setupReturns() {
        when( meta.getDbName() ).thenReturn( "dbname" );
        when( meta.getCollection() ).thenReturn( "collection" );
        when( meta.getAuthenticationUser() ).thenReturn( "joe" );
        mongoFields = asList( mongoField( "foo" ), mongoField( "bar" ), mongoField( "baz" ) );
        when( meta.getMongoFields() ).thenReturn( mongoFields );
        when( meta.getMongoFields() ).thenReturn( mongoFields );
    }

    private MongoDbOutputMeta.MongoField mongoField( String fieldName ) {
        MongoDbOutputMeta.MongoField field = new MongoDbOutputMeta.MongoField();
        field.m_incomingFieldName = fieldName;
        field.m_mongoDocPath = fieldName;
        VariableSpace vars = mock( VariableSpace.class );
        when( vars.environmentSubstitute( anyString() ) ).thenReturn( fieldName );
        when( vars.environmentSubstitute( anyString(), anyBoolean() ) ).thenReturn( fieldName );
        field.init( vars );
        return field;
    }

}
