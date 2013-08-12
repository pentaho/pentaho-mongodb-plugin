/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.mongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta;

import com.mongodb.BasicDBList;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

/**
 * Static utility routines for MongoDB. Initialize and configure a connection,
 * retrieve replica set member info from the system.replset collection, retrieve
 * tag sets defined for replica set members, get any custom getLastErrorModes
 * etc.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MongoUtils {

  private static Class<?> PKG = MongoUtils.class;

  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local"; //$NON-NLS-1$
  public static final String REPL_SET_COLLECTION = "system.replset"; //$NON-NLS-1$
  public static final String REPL_SET_SETTINGS = "settings"; //$NON-NLS-1$
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes"; //$NON-NLS-1$
  public static final String REPL_SET_MEMBERS = "members"; //$NON-NLS-1$

  /**
   * Utility method to configure Mongo connection options
   * 
   * @param optsBuilder an options builder
   * @param connTimeout the connection timeout to use (can be null)
   * @param socketTimeout the socket timeout to use (can be null)
   * @param readPreference the read preference to use (can be null)
   * @param writeConcern the writeConcern to use (can be null)
   * @param wTimeout the w timeout to use (can be null)
   * @param journaled whether to use journaled writes
   * @param tagSet the tag set to use in conjunction with the read preference
   *          (can be null)
   * @param vars variables to use
   * @param log for logging
   * @throws KettleException if a problem occurs
   */
  public static void configureConnectionOptions(
      MongoClientOptions.Builder optsBuilder, String connTimeout,
      String socketTimeout, String readPreference, String writeConcern,
      String wTimeout, boolean journaled, List<String> tagSet,
      VariableSpace vars, LogChannelInterface log) throws KettleException {

    // connection timeout
    if (!Const.isEmpty(connTimeout)) {
      String connS = vars.environmentSubstitute(connTimeout);
      try {
        int cTimeout = Integer.parseInt(connS);
        if (cTimeout > 0) {
          optsBuilder.connectTimeout(cTimeout);
        }
      } catch (NumberFormatException n) {
        throw new KettleException(n);
      }
    }

    // socket timeout
    if (!Const.isEmpty(socketTimeout)) {
      String sockS = vars.environmentSubstitute(socketTimeout);
      try {
        int sockTimeout = Integer.parseInt(sockS);
        if (sockTimeout > 0) {
          optsBuilder.socketTimeout(sockTimeout);
        }
      } catch (NumberFormatException n) {
        throw new KettleException(n);
      }
    }

    if (log != null) {
      String rpLogSetting = "Primary"; //$NON-NLS-1$

      if (!Const.isEmpty(readPreference)) {
        rpLogSetting = readPreference;
      }
      log.logBasic(BaseMessages.getString(PKG,
          "MongoUtils.Message.UsingReadPreference", rpLogSetting)); //$NON-NLS-1$
    }
    DBObject firstTagSet = null;
    DBObject[] remainingTagSets = new DBObject[0];
    if (tagSet != null && tagSet.size() > 0) {
      if (tagSet.size() > 1) {
        remainingTagSets = new DBObject[tagSet.size() - 1];
      }

      firstTagSet = (DBObject) JSON.parse(tagSet.get(0).trim());
      for (int i = 1; i < tagSet.size(); i++) {
        remainingTagSets[i - 1] = (DBObject) JSON.parse(tagSet.get(i).trim());
      }
      if (log != null
          && (!Const.isEmpty(readPreference) && !readPreference
              .equalsIgnoreCase("primary"))) { //$NON-NLS-1$
        StringBuilder builder = new StringBuilder();
        for (String s : tagSet) {
          builder.append(s).append(" "); //$NON-NLS-1$
        }
        log.logBasic(BaseMessages
            .getString(
                PKG,
                "MongoUtils.Message.UsingReadPreferenceTagSets", builder.toString())); //$NON-NLS-1$
      }
    } else {
      if (log != null) {
        log.logBasic(BaseMessages.getString(PKG,
            "MongoUtils.Message.NoReadPreferenceTagSetsDefined")); //$NON-NLS-1$
      }
    }

    // read preference
    if (!Const.isEmpty(readPreference)) {
      String rp = vars.environmentSubstitute(readPreference);

      if (rp.equalsIgnoreCase("Primary")) { //$NON-NLS-1$
        optsBuilder.readPreference(ReadPreference.primary());
      } else if (rp.equalsIgnoreCase("Primary preferred")) { //$NON-NLS-1$
        if (firstTagSet != null) {
          optsBuilder.readPreference(ReadPreference.primaryPreferred(
              firstTagSet, remainingTagSets));
        } else {
          optsBuilder.readPreference(ReadPreference.primaryPreferred());
        }
      } else if (rp.equalsIgnoreCase("Secondary")) { //$NON-NLS-1$
        if (firstTagSet != null) {
          System.out.println("Configuring read preference with tag set."); //$NON-NLS-1$
          optsBuilder.readPreference(ReadPreference.secondary(firstTagSet,
              remainingTagSets));
        } else {
          optsBuilder.readPreference(ReadPreference.secondary());
        }
      } else if (rp.equalsIgnoreCase("Secondary preferred")) { //$NON-NLS-1$
        if (firstTagSet != null) {
          optsBuilder.readPreference(ReadPreference.secondaryPreferred(
              firstTagSet, remainingTagSets));
        } else {
          optsBuilder.readPreference(ReadPreference.secondaryPreferred());
        }
      } else if (rp.equalsIgnoreCase("Nearest")) { //$NON-NLS-1$
        if (firstTagSet != null) {
          optsBuilder.readPreference(ReadPreference.nearest(firstTagSet,
              remainingTagSets));
        } else {
          optsBuilder.readPreference(ReadPreference.nearest());
        }
      }
    }

    // write concern
    writeConcern = vars.environmentSubstitute(writeConcern);
    wTimeout = vars.environmentSubstitute(wTimeout);

    WriteConcern concern = null;

    if (Const.isEmpty(writeConcern) && Const.isEmpty(wTimeout) && !journaled) {
      concern = new WriteConcern();

      // all defaults - timeout 0, journal = false, w = 1
      concern.setWObject(new Integer(1));

      if (log != null) {
        log.logBasic(BaseMessages.getString(PKG,
            "MongoUtils.Message.ConfiguringWithDefaultWriteConcern"));
      }
    } else {
      int wt = 0;
      if (!Const.isEmpty(wTimeout)) {
        try {
          wt = Integer.parseInt(wTimeout);
        } catch (NumberFormatException n) {
          throw new KettleException(n);
        }
      }

      if (!Const.isEmpty(writeConcern)) {
        // try parsing as a number first
        try {
          int wc = Integer.parseInt(writeConcern);
          concern = new WriteConcern(wc, wt, false, journaled);

          if (log != null) {
            String lwc = "w = " + writeConcern + ", wTimeout = " + wt
                + ", journaled = " + (new Boolean(journaled).toString());
            log.logBasic(BaseMessages.getString(PKG,
                "MongoUtils.Message.ConfiguringWithWriteConcern", lwc));
          }
        } catch (NumberFormatException n) {
          // assume its a valid string - e.g. "majority" or a custom
          // getLastError label associated with a tag set
          concern = new WriteConcern(writeConcern, wt, false, journaled);

          if (log != null) {
            String lwc = "w = " + writeConcern + ", wTimeout = " + wt
                + ", journaled = " + (new Boolean(journaled).toString());
            log.logBasic(BaseMessages.getString(PKG,
                "MongoUtils.Message.ConfiguringWithWriteConcern", lwc));
          }
        }
      } else {
        concern = new WriteConcern(1, wt, false, journaled);

        if (log != null) {
          String lwc = "w = 1" + ", wTimeout = " + wt + ", journaled = "
              + (new Boolean(journaled).toString());
          log.logBasic(BaseMessages.getString(PKG,
              "MongoUtils.Message.ConfiguringWithWriteConcern", lwc));
        }
      }
    }
    optsBuilder.writeConcern(concern);
  }

  public static MongoClient initConnection(String hostsPorts,
      String singlePort, String username, String password,
      boolean useAllReplicaSetMembers, String connTimeout,
      String socketTimeout, String readPreference, String writeConcern,
      String wTimeout, boolean journaled, List<String> tagSet,
      VariableSpace vars, LogChannelInterface log) throws KettleException {

    hostsPorts = vars.environmentSubstitute(hostsPorts);
    singlePort = vars.environmentSubstitute(singlePort);
    int singlePortI = -1;

    try {
      singlePortI = Integer.parseInt(singlePort);
    } catch (NumberFormatException n) {
      // don't complain
    }

    if (Const.isEmpty(hostsPorts)) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoUtils.Message.Error.EmptyHostsString")); //$NON-NLS-1$
    }

    List<ServerAddress> repSet = new ArrayList<ServerAddress>();

    if (useAllReplicaSetMembers) {
      repSet = getReplicaSetMembers(hostsPorts, singlePort, username, password,
          vars, log);

      if (repSet.size() == 0) {
        useAllReplicaSetMembers = false; // drop back and just configure using
                                         // what we've been given
      } else {
        if (log != null) {
          StringBuilder builder = new StringBuilder();
          for (ServerAddress s : repSet) {
            builder.append(s.toString()).append(" ");
          }
          log.logBasic(BaseMessages.getString(PKG,
              "MongoUtils.Message.UsingTheFollowingReplicaSetMembers")
              + " "
              + builder.toString());
        }
      }
    }

    if (!useAllReplicaSetMembers) {
      String[] parts = hostsPorts.trim().split(","); //$NON-NLS-1$
      for (String part : parts) {
        // host:port?
        int port = singlePortI != -1 ? singlePortI : MONGO_DEFAULT_PORT;
        String[] hp = part.split(":"); //$NON-NLS-1$
        if (hp.length > 2) {
          throw new KettleException(BaseMessages.getString(PKG,
              "MongoUtils.Message.Error.MalformedHost", part)); //$NON-NLS-1$
        }

        String host = hp[0];
        if (hp.length == 2) {
          // non-default port
          try {
            port = Integer.parseInt(hp[1].trim());
          } catch (NumberFormatException n) {
            throw new KettleException(BaseMessages.getString(PKG,
                "MongoUtils.Message.Error.UnableToParsePortNumber", hp[1])); //$NON-NLS-1$
          }
        }

        try {
          ServerAddress s = new ServerAddress(host, port);
          repSet.add(s);
        } catch (UnknownHostException u) {
          throw new KettleException(u);
        }
      }
    }

    MongoClientOptions.Builder mongoOptsBuilder = new MongoClientOptions.Builder();

    configureConnectionOptions(mongoOptsBuilder, connTimeout, socketTimeout,
        readPreference, writeConcern, wTimeout, journaled, tagSet, vars, log);

    MongoClientOptions opts = mongoOptsBuilder.build();
    try {
      return (repSet.size() > 1 ? new MongoClient(repSet, opts) : (repSet
          .size() == 1 ? new MongoClient(repSet.get(0), opts)
          : new MongoClient(new ServerAddress("localhost"), opts))); //$NON-NLS-1$
    } catch (UnknownHostException u) {
      throw new KettleException(u);
    }
  }

  /**
   * Create a connection to a Mongo server based on parameters supplied in the
   * step meta data
   * 
   * @param meta the step meta data
   * @param vars variables to use
   * @param log for logging
   * @return a configured MongoClient object
   * @throws KettleException if a problem occurs
   */
  public static MongoClient initConnection(MongoDbOutputMeta meta,
      VariableSpace vars, LogChannelInterface log) throws KettleException {

    String hostsPorts = meta.getHostnames();
    String singlePort = meta.getPort();
    String username = meta.getUsername();
    String password = meta.getPassword();
    String connTimeout = meta.getConnectTimeout();
    String sockTimeout = meta.getSocketTimeout();
    String readPreference = meta.getReadPreference();
    String writeConcern = meta.getWriteConcern();
    String wTimeout = meta.getWTimeout();
    boolean journaled = meta.getJournal();
    // no read preference tag sets in the output step
    List<String> tagSet = null;

    boolean useAllReplicaSetMembers = meta.getUseAllReplicaSetMembers();

    return initConnection(hostsPorts, singlePort, username, password,
        useAllReplicaSetMembers, connTimeout, sockTimeout, readPreference,
        writeConcern, wTimeout, journaled, tagSet, vars, log);
  }

  /**
   * Create a connection to a Mongo server based on parameters supplied in the
   * step meta data
   * 
   * @param meta the step meta data
   * @param vars variables to use
   * @param log for logging
   * @return a configured MongoClient object
   * @throws KettleException if a problem occurs
   */
  public static MongoClient initConnection(MongoDbInputMeta meta,
      VariableSpace vars, LogChannelInterface log) throws KettleException {
    String hostsPorts = meta.getHostnames();
    String singlePort = meta.getPort();
    String username = meta.getAuthenticationUser();
    String password = meta.getAuthenticationPassword();
    String connTimeout = meta.getConnectTimeout();
    String sockTimeout = meta.getSocketTimeout();
    String readPreference = meta.getReadPreference();
    String writeConcern = null;
    String wTimeout = null;
    boolean journaled = false;
    List<String> tagSet = meta.getReadPrefTagSets();
    boolean useAllReplicaSetMembers = meta.getUseAllReplicaSetMembers();

    return initConnection(hostsPorts, singlePort, username, password,
        useAllReplicaSetMembers, connTimeout, sockTimeout, readPreference,
        writeConcern, wTimeout, journaled, tagSet, vars, log);
  }

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica
   * set configuration object on the server. These can be used as the "w"
   * setting for the write concern in addition to the standard "w" values of
   * <number> or "majority".
   * 
   * @param hostsPorts the hosts to use
   * @param singlePort the default port to use if no ports are given in the
   *          hostsPorts spec
   * @param username the username for authentication
   * @param password the password for authentication
   * @param vars the environment variables to use
   * @param log for logging
   * @return a list of the names of any custom "lastErrorModes"
   * @throws KettleException if a problem occurs
   */
  public static List<String> getLastErrorModes(String hostsPorts,
      String singlePort, String username, String password, VariableSpace vars,
      LogChannelInterface log) throws KettleException {

    List<String> customLastErrorModes = new ArrayList<String>();

    MongoClient mongo = null;
    try {
      mongo = initConnection(hostsPorts, singlePort, username, password, false,
          null, null, null, null, null, false, null, vars, log);

      username = vars.environmentSubstitute(username);
      String realPass = vars.environmentSubstitute(password);

      DB local = mongo.getDB(LOCAL_DB);
      if (local != null) {

        if (!Const.isEmpty(username) || !Const.isEmpty(realPass)) {
          CommandResult comResult = local.authenticateCommand(username,
              realPass.toCharArray());
          if (!comResult.ok()) {
            throw new KettleException(BaseMessages.getString(PKG,
                "MongoDbOutput.Messages.Error.UnableToAuthenticate", //$NON-NLS-1$
                comResult.getErrorMessage()));
          }
        }

        DBCollection replset = local.getCollection(REPL_SET_COLLECTION);
        if (replset != null) {
          DBObject config = replset.findOne();

          extractLastErrorModes(config, customLastErrorModes);
        }
      }
    } finally {
      if (mongo != null) {
        mongo.close();
      }
    }

    return customLastErrorModes;
  }

  protected static void extractLastErrorModes(DBObject config,
      List<String> customLastErrorModes) {
    if (config != null) {
      Object settings = config.get(REPL_SET_SETTINGS);

      if (settings != null) {
        Object getLastErrModes = ((DBObject) settings)
            .get(REPL_SET_LAST_ERROR_MODES);

        if (getLastErrModes != null) {
          for (String m : ((DBObject) getLastErrModes).keySet()) {
            customLastErrorModes.add(m);
          }
        }
      }
    }
  }

  /**
   * Connect to mongo and retrieve any custom getLastError modes defined in the
   * local.system.replset collection
   * 
   * @param meta the MongoDbOutputMeta containing settings to use
   * @param vars environment variables
   * @param log for logging
   * @return a list containing any custom getLastError modes
   * @throws KettleException if a connection or authentication error occurs
   */
  public static List<String> getLastErrorModes(MongoDbOutputMeta meta,
      VariableSpace vars, LogChannelInterface log) throws KettleException {

    return getLastErrorModes(meta.getHostnames(), meta.getPort(),
        meta.getUsername(), meta.getPassword(), vars, log);
  }

  protected static String quote(String string) {
    if (string.indexOf('"') >= 0) {

      if (string.indexOf('"') >= 0) {
        string = string.replace("\"", "\\\""); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }

    string = ("\"" + string + "\""); //$NON-NLS-1$ //$NON-NLS-2$

    return string;
  }

  /**
   * Get a list of all tagName : tagValue pairs that occur in the tag sets
   * defined across the replica set.
   * 
   * @param meta the MongoDbInput containing settings to use
   * @param vars environment variables to use
   * @param log for logging
   * @return a list of tags that occur in the replica set configuration
   * @throws KettleException if a problem occurs
   */
  public static List<String> getAllTags(MongoDbInputMeta meta,
      VariableSpace vars, LogChannelInterface log) throws KettleException {
    return getAllTags(meta.getHostnames(), meta.getPort(),
        meta.getAuthenticationUser(), meta.getAuthenticationPassword(), vars,
        log);
  }

  /**
   * Get a list of all tagName : tagValue pairs that occur in the tag sets
   * defined across the replica set.
   * 
   * @param hostsPorts the hosts to use
   * @param singlePort the default port to use if no ports specified in
   *          hostsPorts spec
   * @param username the username to authenticate with
   * @param password the password to authenticate with
   * @param vars environment variables to use
   * @param log for logging
   * @return a list of tags that occur in the replica set configuration
   * @throws KettleException if an error occurs
   */
  public static List<String> getAllTags(String hostsPorts, String singlePort,
      String username, String password, VariableSpace vars,
      LogChannelInterface log) throws KettleException {

    List<String> allTags = new ArrayList<String>();

    BasicDBList members = getRepSetMemberRecords(hostsPorts, singlePort,
        username, password, vars, log);

    setupAllTags(members, allTags);

    return allTags;
  }

  protected static void setupAllTags(BasicDBList members, List<String> allTags) {
    HashSet<String> tempTags = new HashSet<String>();

    if (members != null && members.size() > 0) {
      for (int i = 0; i < members.size(); i++) {
        Object m = members.get(i);

        if (m != null) {
          DBObject tags = (DBObject) ((DBObject) m).get("tags"); //$NON-NLS-1$
          if (tags == null) {
            continue;
          }

          for (String tagName : tags.keySet()) {
            String tagVal = tags.get(tagName).toString();
            String combined = quote(tagName) + " : " + quote(tagVal); //$NON-NLS-1$
            tempTags.add(combined);
          }
        }
      }
    }

    for (String s : tempTags) {
      allTags.add(s);
    }
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of
   * tag set. It is assumed that members satisfy according to an OR relationship
   * = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   * 
   * @param tagSets the list of tag sets to match against
   * @param meta MongoDbInput meta
   * @param vars environment variables to use
   * @param log for logging
   * @return a list of replica set members who's tags satisfy the supplied list
   *         of tag sets
   * @throws KettleException if a problem occurs
   */
  public static List<DBObject> getReplicaSetMembersThatSatisfyTagSets(
      List<DBObject> tagSets, MongoDbInputMeta meta, VariableSpace vars,
      LogChannelInterface log) throws KettleException {
    return getReplicaSetMembersThatSatisfyTagSets(tagSets, meta.getHostnames(),
        meta.getPort(), meta.getAuthenticationUser(),
        meta.getAuthenticationPassword(), vars, log);
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of
   * tag set. It is assumed that members satisfy according to an OR relationship
   * = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   * 
   * @param tagSets the list of tag sets to match against
   * @param hostsPorts the hosts to use
   * @param singlePort the default port to use, if no ports are given in the
   *          hostsPorts spec
   * @param username the username for authentication
   * @param password the password for authentication
   * @param vars environment variables to use
   * @param log for logging
   * @return a list of replica set members who's tags satisfy the supplied list
   *         of tag sets
   * @throws KettleException if a problem occurs
   */
  public static List<DBObject> getReplicaSetMembersThatSatisfyTagSets(
      List<DBObject> tagSets, String hostsPorts, String singlePort,
      String username, String password, VariableSpace vars,
      LogChannelInterface log) throws KettleException {

    List<DBObject> satisfy = new ArrayList<DBObject>();

    BasicDBList members = getRepSetMemberRecords(hostsPorts, singlePort,
        username, password, vars, log);

    checkForReplicaSetMembersThatSatisfyTagSets(tagSets, satisfy, members);

    return satisfy;
  }

  protected static void checkForReplicaSetMembersThatSatisfyTagSets(
      List<DBObject> tagSets, List<DBObject> satisfy, BasicDBList members) {
    if (members != null && members.size() > 0) {
      for (int i = 0; i < members.size(); i++) {
        Object m = members.get(i);

        if (m != null) {
          DBObject tags = (DBObject) ((DBObject) m).get("tags"); //$NON-NLS-1$
          if (tags == null) {
            continue;
          }

          for (int j = 0; j < tagSets.size(); j++) {
            boolean match = true;
            DBObject toMatch = tagSets.get(j);

            for (String tagName : toMatch.keySet()) {
              String tagValue = toMatch.get(tagName).toString();

              // does replica set member m's tags contain this tag?
              Object matchVal = tags.get(tagName);

              if (matchVal == null) {
                match = false; // doesn't match this particular tag set
                // no need to check any other keys in toMatch
                break;
              }

              if (!matchVal.toString().equals(tagValue)) {
                // rep set member m's tags has this tag, but it's value does not
                // match
                match = false;

                // no need to check any other keys in toMatch
                break;
              }
            }

            if (match) {
              // all tag/values present and match - add this member (only if its
              // not already there)
              if (!satisfy.contains(m)) {
                satisfy.add((DBObject) m);
              }
            }
          }
        }
      }
    }
  }

  protected static BasicDBList getRepSetMemberRecords(String hostsPorts,
      String singlePort, String username, String password, VariableSpace vars,
      LogChannelInterface log) throws KettleException {

    MongoClient mongo = null;
    BasicDBList setMembers = null;
    try {
      mongo = initConnection(hostsPorts, singlePort, username, password, false,
          null, null, null, null, null, false, null, vars, log);

      username = vars.environmentSubstitute(username);
      String realPass = vars.environmentSubstitute(password);

      DB local = mongo.getDB(LOCAL_DB);
      if (local != null) {

        if (!Const.isEmpty(username) || !Const.isEmpty(realPass)) {
          CommandResult comResult = local.authenticateCommand(username,
              realPass.toCharArray());
          if (!comResult.ok()) {
            throw new KettleException(BaseMessages.getString(PKG,
                "MongoDbOutput.Messages.Error.UnableToAuthenticate", //$NON-NLS-1$
                comResult.getErrorMessage()));
          }
        }

        DBCollection replset = local.getCollection(REPL_SET_COLLECTION);
        if (replset != null) {
          DBObject config = replset.findOne();

          if (config != null) {
            Object members = config.get(REPL_SET_MEMBERS);

            if (members instanceof BasicDBList) {
              if (((BasicDBList) members).size() == 0) {
                // log that there are no replica set members defined
                if (log != null) {
                  log.logBasic(BaseMessages.getString(PKG,
                      "MongoUtils.Message.Warning.NoReplicaSetMembersDefined")); //$NON-NLS-1$
                }
              } else {
                setMembers = (BasicDBList) members;
              }

            } else {
              // log that there are no replica set members defined
              if (log != null) {
                log.logBasic(BaseMessages.getString(PKG,
                    "MongoUtils.Message.Warning.NoReplicaSetMembersDefined")); //$NON-NLS-1$
              }
            }
          } else {
            // log that there are no replica set members defined
            if (log != null) {
              log.logBasic(BaseMessages.getString(PKG,
                  "MongoUtils.Message.Warning.NoReplicaSetMembersDefined")); //$NON-NLS-1$
            }
          }
        } else {
          // log that the replica set collection is not available
          if (log != null) {
            log.logBasic(BaseMessages.getString(PKG,
                "MongoUtils.Message.Warning.ReplicaSetCollectionUnavailable")); //$NON-NLS-1$
          }
        }
      } else {
        // log that the local database is not available!!
        if (log != null) {
          log.logBasic(BaseMessages.getString(PKG,
              "MongoUtils.Message.Warning.LocalDBNotAvailable")); //$NON-NLS-1$
        }
      }
    } catch (Exception ex) {
      throw new KettleException(ex);
    } finally {
      if (mongo != null) {
        mongo.close();
      }
    }

    return setMembers;
  }

  /**
   * Connect to mongo and retrieve any replica set members defined in the
   * local.system.replset collection
   * 
   * @param hostsPorts the host(s) and port(s) to use for initiating the
   *          connection
   * @param singlePort default port to use if none specified in the hostsPorts
   *          string
   * @param username username to use for authenticating
   * @param password password to use for authenticating
   * @param vars environment variables
   * @param log for logging
   * @return a list of replica set ServerAddresses
   * @throws KettleException if a problem occurs
   */
  public static List<ServerAddress> getReplicaSetMembers(String hostsPorts,
      String singlePort, String username, String password, VariableSpace vars,
      LogChannelInterface log) throws KettleException {
    List<ServerAddress> replSetMembers = new ArrayList<ServerAddress>();

    if (log != null) {
      log.logBasic(BaseMessages.getString(PKG,
          "MongoUtils.Message.QueryingForReplicaSetMembers", hostsPorts));
    }
    BasicDBList members = getRepSetMemberRecords(hostsPorts, singlePort,
        username, password, vars, log);

    try {
      if (members != null && members.size() > 0) {
        for (int i = 0; i < members.size(); i++) {
          Object m = members.get(i);

          if (m != null) {
            String hostPort = ((DBObject) m).get("host").toString(); //$NON-NLS-1$
            if (!Const.isEmpty(hostPort)) {
              String[] parts = hostPort.split(":"); //$NON-NLS-1$
              if (parts.length == 2) {
                ServerAddress address = new ServerAddress(parts[0].trim(),
                    Integer.parseInt(parts[1].trim()));
                replSetMembers.add(address);
              } else {
                ServerAddress address = new ServerAddress(parts[0].trim());
                replSetMembers.add(address);
              }
            }
          }
        }
      }
    } catch (Exception ex) {
      throw new KettleException(ex);
    }

    return replSetMembers;
  }

  public static void main(String[] args) {
    try {
      String hostPort = args[0];
      String defaultPort = args[1];
      Variables vars = new Variables();

      List<String> repSetTags = MongoUtils.getAllTags(hostPort, defaultPort,
          null, null, vars, null);

      System.out.println("Number of tags: " + repSetTags.size()); //$NON-NLS-1$
      for (String tag : repSetTags) {
        System.out.println(tag);
      }

      List<ServerAddress> repSetMembers = MongoUtils.getReplicaSetMembers(
          hostPort, defaultPort, null, null, vars, null);
      System.out.println("Number of replica set members: "
          + repSetMembers.size());
      for (ServerAddress s : repSetMembers) {
        System.out.println(s.toString());
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
