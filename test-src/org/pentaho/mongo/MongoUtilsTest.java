package org.pentaho.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongoUtilsTest {
  public static String REP_SET_CONFIG = "{\"_id\" : \"foo\", \"version\" : 1, "
      + "\"members\" : [" + "{" + "\"_id\" : 0, "
      + "\"host\" : \"palladium.lan:27017\", " + "\"tags\" : {"
      + "\"dc.one\" : \"primary\", " + "\"use\" : \"production\"" + "}" + "}, "
      + "{" + "\"_id\" : 1, " + "\"host\" : \"palladium.local:27018\", "
      + "\"tags\" : {" + "\"dc.two\" : \"slave1\"" + "}" + "}, " + "{"
      + "\"_id\" : 2, " + "\"host\" : \"palladium.local:27019\", "
      + "\"tags\" : {" + "\"dc.three\" : \"slave2\", "
      + "\"use\" : \"production\"" + "}" + "}" + "]," + "\"settings\" : {"
      + "\"getLastErrorModes\" : { " + "\"DCThree\" : {" + "\"dc.three\" : 1"
      + "}" + "}" + "}" + "}";

  public static String TAG_SET = "{\"use\" : \"production\"}";

  @Test
  public void testExtractLastErrorMode() {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);

    assertTrue(config != null);
    List<String> lastErrorModes = new ArrayList<String>();

    MongoUtils.extractLastErrorModes(config, lastErrorModes);

    assertTrue(lastErrorModes.size() == 1);
    assertEquals("DCThree", lastErrorModes.get(0));
  }

  @Test
  public void testGetAllReplicaSetMemberRecords() {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(MongoUtils.REPL_SET_MEMBERS);

    assertTrue(members != null);
    assertTrue(members instanceof BasicDBList);
    assertEquals(3, ((BasicDBList) members).size());
  }

  @Test
  public void testGetAllTags() {
    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(MongoUtils.REPL_SET_MEMBERS);

    List<String> allTags = new ArrayList<String>();

    MongoUtils.setupAllTags((BasicDBList) members, allTags);

    assertEquals(4, allTags.size());
  }

  @Test
  public void testGetReplicaSetMembersThatSatisfyTagSets() {
    List<DBObject> satisfy = new ArrayList<DBObject>();
    List<DBObject> tagSets = new ArrayList<DBObject>(); // tags to satisfy

    DBObject tSet = (DBObject) JSON.parse(TAG_SET);
    tagSets.add(tSet);

    DBObject config = (DBObject) JSON.parse(REP_SET_CONFIG);
    Object members = config.get(MongoUtils.REPL_SET_MEMBERS);

    MongoUtils.checkForReplicaSetMembersThatSatisfyTagSets(tagSets, satisfy,
        (BasicDBList) members);

    // two replica set members have the "use : production" tag in their tag sets
    assertEquals(2, satisfy.size());
  }

  public static void main(String[] args) {
    try {
      MongoUtilsTest test = new MongoUtilsTest();

      test.testExtractLastErrorMode();
      test.testGetAllReplicaSetMemberRecords();
      test.testGetAllTags();
      test.testGetReplicaSetMembersThatSatisfyTagSets();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
