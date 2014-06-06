package org.pentaho.di.trans.steps.mongodbdelete;

import static org.junit.Assert.assertEquals;

import com.mongodb.DBObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Unit tests for MongoDbDelete
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
public class MongoDbDeleteTest {

    @BeforeClass
    public static void beforeClass() throws KettlePluginException {
        PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
        PluginRegistry.init();
    }

    @Test
    public void testSimpleDeleteQuery() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field";
        mongoField.m_comparator = "=";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta("incomming_field", ValueMetaInterface.TYPE_STRING);
        rmi.addValueMeta(vm);

        Object[] row = new Object[1];
        row[0] = "ADAGG";

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : \"ADAGG\"}");
    }

    @Test
    public void testNestedDocDeleteQuery() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field";
        mongoField.m_comparator = "=";
        mongoField.m_mongoDocPath = "details.id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta("incomming_field", ValueMetaInterface.TYPE_STRING);
        rmi.addValueMeta(vm);

        Object[] row = new Object[1];
        row[0] = "ADAGG";

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"details.id\" : \"ADAGG\"}");
    }

    @Test
    public void testSimpleDeleteGreaterThanQuery() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field";
        mongoField.m_comparator = ">";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta("incomming_field", ValueMetaInterface.TYPE_STRING);
        rmi.addValueMeta(vm);

        Object[] row = new Object[1];
        row[0] = "ADAGG";

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$gt\" : \"ADAGG\"}}");
    }

    @Test
    public void testSimpleDeleteLessThanQuery() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field";
        mongoField.m_comparator = "<";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta("incomming_field", ValueMetaInterface.TYPE_STRING);
        rmi.addValueMeta(vm);

        Object[] row = new Object[1];
        row[0] = "ADAGG";

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$lt\" : \"ADAGG\"}}");
    }

    @Test
    public void testSimpleDeleteQueryBetween() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field1";
        mongoField.m_incomingField2 = "incomming_field2";
        mongoField.m_comparator = "BETWEEN";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        ValueMetaInterface vm2 = ValueMetaFactory.createValueMeta("incomming_field2", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);
        rmi.addValueMeta(vm2);

        Object[] row = new Object[2];
        row[0] = new Long(1);
        row[1] = new Long(10);

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$gt\" : 1 , \"$lt\" : 10}}");
    }

    @Test
    public void testSimpleDeleteQueryBetweenIgnoreIfNull() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field1";
        mongoField.m_incomingField2 = "incomming_field2";
        mongoField.m_comparator = "BETWEEN";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        ValueMetaInterface vm2 = ValueMetaFactory.createValueMeta("incomming_field2", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);
        rmi.addValueMeta(vm2);

        Object[] row = new Object[2];
        row[0] = new Long(1);
        row[1] = null;

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$gt\" : 1}}");
    }

    @Test(expected = KettleException.class)
    public void testSimpleDeleteQueryBetweenRequiredTwoFields() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field1";
        mongoField.m_comparator = "BETWEEN";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);

        Object[] row = new Object[2];
        row[0] = new Long(1);

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);
    }

    @Test
    public void testSimpleDeleteQueryIsNull() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field1";
        mongoField.m_comparator = "IS NULL";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);

        Object[] row = new Object[2];
        row[0] = null;

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$exists\" : false}}");
    }

    @Test
    public void testSimpleDeleteQueryIsNotNull() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField = new MongoDbDeleteMeta.MongoField();
        mongoField.m_incomingField1 = "incomming_field1";
        mongoField.m_comparator = "IS NOT NULL";
        mongoField.m_mongoDocPath = "id";
        paths.add(mongoField);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);

        Object[] row = new Object[2];
        row[0] = null;

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$exists\" : true}}");
    }

    @Test
    public void testSimpleDeleteQueryCombineIsNotNullAndEqual() throws KettleException {
        List<MongoDbDeleteMeta.MongoField> paths = new ArrayList<MongoDbDeleteMeta.MongoField>();

        MongoDbDeleteMeta.MongoField mongoField1 = new MongoDbDeleteMeta.MongoField();
        mongoField1.m_incomingField1 = "incomming_field1";
        mongoField1.m_comparator = "IS NOT NULL";
        mongoField1.m_mongoDocPath = "id";
        paths.add(mongoField1);

        MongoDbDeleteMeta.MongoField mongoField2 = new MongoDbDeleteMeta.MongoField();
        mongoField2.m_incomingField1 = "incomming_field1";
        mongoField2.m_comparator = "=";
        mongoField2.m_mongoDocPath = "data";
        paths.add(mongoField2);

        RowMetaInterface rmi = new RowMeta();
        ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta("incomming_field1", ValueMetaInterface.TYPE_INTEGER);
        rmi.addValueMeta(vm1);

        Object[] row = new Object[2];
        row[0] = new Long(10);

        Variables vs = new Variables();

        for (MongoDbDeleteMeta.MongoField field : paths) {
            field.init(vs);
        }

        DBObject result = MongoDbDeleteData.getQueryObject(paths, rmi, row, vs);

        assertEquals(result.toString(), "{ \"id\" : { \"$exists\" : true} , \"data\" : 10}");
    }

    @Test(expected = KettleException.class)
    public void testStepIsFailedIfOneOfMongoFieldsNotFound() throws KettleException {
        MongoDbDelete delete = prepareMongoDbDeleteMock();

        final String[] metaNames = new String[]{"a1", "a2", "a3"};
        String[] mongoNames1 = new String[]{"b1", "a2", "a3"};
        String[] mongoNames2 = new String[]{"c1", "a2", "a3"};
        List<MongoDbDeleteMeta.MongoField> mongoFields = getMongoFields(mongoNames1, mongoNames2);
        RowMetaInterface rmi = getStubRowMetaInterface(metaNames);
        EasyMock.replay(delete);

        try {
            delete.checkInputFieldsMatch(rmi, mongoFields);
        } catch (Exception e) {
            String detailedMessage = e.getMessage();
            Assert.assertTrue("Exception message mentions fields not found:", detailedMessage.contains("b1"));
            throw new KettleException(e);
        }
    }

    @Test(expected = KettleException.class)
    public void testStepFailsIfNoMongoFieldsFound() throws KettleException {
        MongoDbDelete output = prepareMongoDbDeleteMock();

        final String[] metaNames = new String[]{"a1", "a2"};
        String[] mongoNames = new String[]{};
        String[] mongoNames2 = new String[]{};
        EasyMock.replay(output);
        RowMetaInterface rmi = getStubRowMetaInterface(metaNames);
        List<MongoDbDeleteMeta.MongoField> mongoFields = getMongoFields(mongoNames, mongoNames2);
        output.checkInputFieldsMatch(rmi, mongoFields);
    }

    @Test
    public void testStepMetaSettingConnectionSetsInXML() throws ParserConfigurationException,
            UnsupportedEncodingException, SAXException, IOException, KettleXMLException {
        MongoDbDeleteMeta mongoMeta = new MongoDbDeleteMeta();
        mongoMeta.setHostnames("172.0.0.1");
        mongoMeta.setPort("22");
        mongoMeta.setDbName("TestDB");
        mongoMeta.setJournal(true);
        mongoMeta.setWriteConcern("SAFE");
        mongoMeta.setUseKerberosAuthentication(false);
        mongoMeta.setUseAllReplicaSetMembers(false);
        mongoMeta.setSocketTimeout("1000");

        String XML = mongoMeta.getXML();
        XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<step>\n" + XML + "\n</step>\n";

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new ByteArrayInputStream(XML.getBytes("UTF-8")));

        NodeList step = doc.getElementsByTagName("step");
        Node stepNode = step.item(0);

        MongoDbDeleteMeta newMeta = new MongoDbDeleteMeta();
        newMeta.loadXML(stepNode, (List<DatabaseMeta>) null, (IMetaStore) null);

        assertEquals("172.0.0.1", newMeta.getHostnames());
        assertEquals("22", newMeta.getPort());
        assertEquals("TestDB", newMeta.getDbName());
        assertTrue(newMeta.getJournal());
        assertEquals("SAFE", newMeta.getWriteConcern());
        assertTrue(!newMeta.getUseKerberosAuthentication());
        assertTrue(!newMeta.getUseAllReplicaSetMembers());
        assertEquals("1000", newMeta.getSocketTimeout());
    }

    private MongoDbDelete prepareMongoDbDeleteMock() {
        MongoDbDelete delete = EasyMock.createNiceMock(MongoDbDelete.class);
        EasyMock.expect(delete.environmentSubstitute(EasyMock.anyObject(String.class))).andAnswer(new IAnswer<String>() {
            @Override
            public String answer() throws Throwable {
                Object[] args = EasyMock.getCurrentArguments();
                String ret = String.valueOf(args[0]);
                return ret;
            }
        }).anyTimes();
        return delete;
    }

    private List<MongoDbDeleteMeta.MongoField> getMongoFields(String[] fieldNames1, String[] fieldNames2) {
        List<MongoDbDeleteMeta.MongoField> ret = new ArrayList<MongoDbDeleteMeta.MongoField>(fieldNames1.length);
        for (String name : fieldNames1) {
            MongoDbDeleteMeta.MongoField field = new MongoDbDeleteMeta.MongoField();
            field.m_incomingField1 = name;
            ret.add(field);
        }

        for (String name : fieldNames2) {
            MongoDbDeleteMeta.MongoField field = new MongoDbDeleteMeta.MongoField();
            field.m_incomingField2 = name;
            ret.add(field);
        }
        return ret;
    }

    private RowMetaInterface getStubRowMetaInterface(final String[] metaNames) {
        RowMetaInterface rmi = EasyMock.createNiceMock(RowMetaInterface.class);
        EasyMock.expect(rmi.getFieldNames()).andReturn(metaNames).anyTimes();
        EasyMock.expect(rmi.size()).andReturn(metaNames.length).anyTimes();
        EasyMock.expect(rmi.getValueMeta(EasyMock.anyInt())).andAnswer(new IAnswer<ValueMetaInterface>() {
            @Override
            public ValueMetaInterface answer() throws Throwable {
                Object[] args = EasyMock.getCurrentArguments();
                int i = Integer.class.cast(args[0]);
                ValueMetaInterface ret = new ValueMetaString(metaNames[i]);
                return ret;
            }
        }).anyTimes();
        EasyMock.replay(rmi);
        return rmi;
    }
}
