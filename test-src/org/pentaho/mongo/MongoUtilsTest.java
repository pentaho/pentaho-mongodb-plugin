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
