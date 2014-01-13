package org.pentaho.mongo.wrapper;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.collection.DefaultMongoCollectionWrapper;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;
import org.pentaho.mongo.wrapper.field.MongoField;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.TaggableReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

public class NoAuthMongoClientWrapper implements MongoClientWrapper {
  private static Class<?> PKG = NoAuthMongoClientWrapper.class;
  public static final int MONGO_DEFAULT_PORT = 27017;

  public static final String LOCAL_DB = "local"; //$NON-NLS-1$
  public static final String REPL_SET_COLLECTION = "system.replset"; //$NON-NLS-1$
  public static final String REPL_SET_SETTINGS = "settings"; //$NON-NLS-1$
  public static final String REPL_SET_LAST_ERROR_MODES = "getLastErrorModes"; //$NON-NLS-1$
  public static final String REPL_SET_MEMBERS = "members"; //$NON-NLS-1$

  private final MongoClient mongo;
  private final LogChannelInterface log;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the step meta data
   * 
   * @param meta
   *          the step meta data
   * @param vars
   *          variables to use
   * @param cred
   *          a configured MongoCredential for authentication (or null for no authentication)
   * @param log
   *          for logging
   * @return a configured MongoClient object
   * @throws KettleException
   *           if a problem occurs
   */
  public NoAuthMongoClientWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log ) throws KettleException {
    this.log = log;
    mongo = initConnection( meta, vars, log );
  }

  public NoAuthMongoClientWrapper( MongoClient mongo, LogChannelInterface log ) {
    this.mongo = mongo;
    this.log = log;
  }

  public MongoClient getMongo() {
    return mongo;
  }

  private MongoClient initConnection( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log )
    throws KettleException {
    String hostsPorts = vars.environmentSubstitute( meta.getHostnames() );
    String singlePort = vars.environmentSubstitute( meta.getPort() );
    String connTimeout = meta.getConnectTimeout();
    String socketTimeout = meta.getSocketTimeout();
    String readPreference = meta.getReadPreference();
    String writeConcern = meta.getWriteConcern();
    String wTimeout = meta.getWTimeout();
    boolean journaled = meta.getJournal();
    List<String> tagSet = meta.getReadPrefTagSets();
    boolean useAllReplicaSetMembers = meta.getUseAllReplicaSetMembers();
    int singlePortI = -1;

    try {
      singlePortI = Integer.parseInt( singlePort );
    } catch ( NumberFormatException n ) {
      // don't complain
    }

    if ( Const.isEmpty( hostsPorts ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.EmptyHostsString" ) ); //$NON-NLS-1$
    }

    List<ServerAddress> repSet = new ArrayList<ServerAddress>();

    // if (useAllReplicaSetMembers) {
    // repSet = getReplicaSetMembers(hostsPorts, singlePort, cred, vars, log);
    //
    // if (repSet.size() == 0) {
    // useAllReplicaSetMembers = false; // drop back and just configure using
    // // what we've been given
    // } else {
    // if (log != null) {
    // StringBuilder builder = new StringBuilder();
    // for (ServerAddress s : repSet) {
    // builder.append(s.toString()).append(" ");
    // }
    // log.logBasic(BaseMessages.getString(PKG,
    // "MongoUtils.Message.UsingTheFollowingReplicaSetMembers")
    // + " "
    // + builder.toString());
    // }
    // }
    // }

    // if (!useAllReplicaSetMembers) {
    String[] parts = hostsPorts.trim().split( "," ); //$NON-NLS-1$
    for ( String part : parts ) {
      // host:port?
      int port = singlePortI != -1 ? singlePortI : MONGO_DEFAULT_PORT;
      String[] hp = part.split( ":" ); //$NON-NLS-1$
      if ( hp.length > 2 ) {
        throw new KettleException( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.MalformedHost", part ) ); //$NON-NLS-1$
      }

      String host = hp[0];
      if ( hp.length == 2 ) {
        // non-default port
        try {
          port = Integer.parseInt( hp[1].trim() );
        } catch ( NumberFormatException n ) {
          throw new KettleException( BaseMessages.getString( PKG,
              "MongoNoAuthWrapper.Message.Error.UnableToParsePortNumber", hp[1] ) ); //$NON-NLS-1$
        }
      }

      try {
        ServerAddress s = new ServerAddress( host, port );
        repSet.add( s );
      } catch ( UnknownHostException u ) {
        throw new KettleException( u );
      }
    }
    // }

    MongoClientOptions.Builder mongoOptsBuilder = new MongoClientOptions.Builder();

    configureConnectionOptions( mongoOptsBuilder, connTimeout, socketTimeout, readPreference, writeConcern, wTimeout,
        journaled, tagSet, vars, log );

    MongoClientOptions opts = mongoOptsBuilder.build();
    return getClient( meta, vars, log, repSet, useAllReplicaSetMembers, opts );
  }

  protected MongoClient getClient( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log,
      List<ServerAddress> repSet, boolean useAllReplicaSetMembers, MongoClientOptions opts ) throws KettleException {
    try {
      // Mongo's java driver will discover all replica set or shard
      // members (Mongos) automatically when MongoClient is constructed
      // using a list of ServerAddresses. The javadocs state that MongoClient
      // should be constructed using a SingleServer address instance (rather
      // than a list) when connecting to a stand-alone host - this is why
      // we differentiate here between a list containing one ServerAddress
      // and a single ServerAddress instance via the useAllReplicaSetMembers
      // flag.
      return ( repSet.size() > 1 || ( useAllReplicaSetMembers && repSet.size() >= 1 ) ? new MongoClient( repSet, opts )
          : ( repSet.size() == 1 ? new MongoClient( repSet.get( 0 ), opts ) : new MongoClient( new ServerAddress(
              "localhost" ), opts ) ) ); //$NON-NLS-1$
    } catch ( UnknownHostException u ) {
      throw new KettleException( u );
    }
  }

  /**
   * Utility method to configure Mongo connection options
   * 
   * @param optsBuilder
   *          an options builder
   * @param connTimeout
   *          the connection timeout to use (can be null)
   * @param socketTimeout
   *          the socket timeout to use (can be null)
   * @param readPreference
   *          the read preference to use (can be null)
   * @param writeConcern
   *          the writeConcern to use (can be null)
   * @param wTimeout
   *          the w timeout to use (can be null)
   * @param journaled
   *          whether to use journaled writes
   * @param tagSet
   *          the tag set to use in conjunction with the read preference (can be null)
   * @param vars
   *          variables to use
   * @param log
   *          for logging
   * @throws KettleException
   *           if a problem occurs
   */
  private void configureConnectionOptions( MongoClientOptions.Builder optsBuilder, String connTimeout,
      String socketTimeout, String readPreference, String writeConcern, String wTimeout, boolean journaled,
      List<String> tagSet, VariableSpace vars, LogChannelInterface log ) throws KettleException {

    // connection timeout
    if ( !Const.isEmpty( connTimeout ) ) {
      String connS = vars.environmentSubstitute( connTimeout );
      try {
        int cTimeout = Integer.parseInt( connS );
        if ( cTimeout > 0 ) {
          optsBuilder.connectTimeout( cTimeout );
        }
      } catch ( NumberFormatException n ) {
        throw new KettleException( n );
      }
    }

    // socket timeout
    if ( !Const.isEmpty( socketTimeout ) ) {
      String sockS = vars.environmentSubstitute( socketTimeout );
      try {
        int sockTimeout = Integer.parseInt( sockS );
        if ( sockTimeout > 0 ) {
          optsBuilder.socketTimeout( sockTimeout );
        }
      } catch ( NumberFormatException n ) {
        throw new KettleException( n );
      }
    }

    if ( log != null ) {
      String rpLogSetting = NamedReadPreference.PRIMARY.getName();

      if ( !Const.isEmpty( readPreference ) ) {
        rpLogSetting = readPreference;
      }
      log.logBasic( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.UsingReadPreference", rpLogSetting ) ); //$NON-NLS-1$
    }
    DBObject firstTagSet = null;
    DBObject[] remainingTagSets = new DBObject[0];
    if ( tagSet != null && tagSet.size() > 0 ) {
      if ( tagSet.size() > 1 ) {
        remainingTagSets = new DBObject[tagSet.size() - 1];
      }

      firstTagSet = (DBObject) JSON.parse( tagSet.get( 0 ).trim() );
      for ( int i = 1; i < tagSet.size(); i++ ) {
        remainingTagSets[i - 1] = (DBObject) JSON.parse( tagSet.get( i ).trim() );
      }
      if ( log != null
          && ( !Const.isEmpty( readPreference ) && !readPreference.equalsIgnoreCase( NamedReadPreference.PRIMARY
              .getName() ) ) ) {
        StringBuilder builder = new StringBuilder();
        for ( String s : tagSet ) {
          builder.append( s ).append( " " ); //$NON-NLS-1$
        }
        log.logBasic( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.Message.UsingReadPreferenceTagSets", builder.toString() ) ); //$NON-NLS-1$
      }
    } else {
      if ( log != null ) {
        log.logBasic( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.NoReadPreferenceTagSetsDefined" ) ); //$NON-NLS-1$
      }
    }

    // read preference
    if ( !Const.isEmpty( readPreference ) ) {

      String rp = vars.environmentSubstitute( readPreference );

      NamedReadPreference preference = NamedReadPreference.byName( rp );

      if ( ( firstTagSet != null ) && ( preference.getPreference() instanceof TaggableReadPreference ) ) {
        optsBuilder.readPreference( preference.getTaggableReadPreference( firstTagSet, remainingTagSets ) );
      } else {
        optsBuilder.readPreference( preference.getPreference() );
      }

    }

    // write concern
    writeConcern = vars.environmentSubstitute( writeConcern );
    wTimeout = vars.environmentSubstitute( wTimeout );

    WriteConcern concern = null;

    if ( Const.isEmpty( writeConcern ) && Const.isEmpty( wTimeout ) && !journaled ) {
      // all defaults - timeout 0, journal = false, w = 1
      concern = new WriteConcern();
      concern.setWObject( new Integer( 1 ) );

      if ( log != null ) {
        log.logBasic( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.ConfiguringWithDefaultWriteConcern" ) ); //$NON-NLS-1$
      }
    } else {
      int wt = 0;
      if ( !Const.isEmpty( wTimeout ) ) {
        try {
          wt = Integer.parseInt( wTimeout );
        } catch ( NumberFormatException n ) {
          throw new KettleException( n );
        }
      }

      if ( !Const.isEmpty( writeConcern ) ) {
        // try parsing as a number first
        try {
          int wc = Integer.parseInt( writeConcern );
          concern = new WriteConcern( wc, wt, false, journaled );
        } catch ( NumberFormatException n ) {
          // assume its a valid string - e.g. "majority" or a custom
          // getLastError label associated with a tag set
          concern = new WriteConcern( writeConcern, wt, false, journaled );
        }
      } else {
        concern = new WriteConcern( 1, wt, false, journaled );
      }

      if ( log != null ) {
        String lwc =
            "w = " + concern.getW() + ", wTimeout = " + concern.getWtimeout() + ", journaled = " + concern.getJ();
        log.logBasic( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.ConfiguringWithWriteConcern", lwc ) );
      }
    }
    optsBuilder.writeConcern( concern );
  }

  /**
   * Retrieve all database names found in MongoDB as visible by the authenticated user.
   * 
   * @throws KettleException
   */
  public List<String> getDatabaseNames() throws KettleException {
    try {
      return getMongo().getDatabaseNames();
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e );
      }
    }
  }

  protected DB getDb( String dbName ) throws KettleException {
    try {
      return getMongo().getDB( dbName );
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e );
      }
    }
  }

  /**
   * Get the set of collections for a MongoDB database.
   * 
   * @param meta
   *          Input meta with connection information
   * @param varSpace
   *          Variable space to substitute variables with
   * @param dB
   *          Name of database
   * @param username
   *          Username to request collections on behalf of
   * @param realPass
   *          Password of user
   * @return Set of collections in the database requested.
   * @throws KettleException
   *           If an error occurs.
   */
  public Set<String> getCollectionsNames( String dB ) throws KettleException {
    try {
      return getDb( dB ).getCollectionNames();
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e );
      }
    }
  }

  /**
   * Return a list of custom "lastErrorModes" (if any) defined in the replica set configuration object on the server.
   * These can be used as the "w" setting for the write concern in addition to the standard "w" values of <number> or
   * "majority".
   * 
   * @return a list of the names of any custom "lastErrorModes"
   * @throws KettleException
   *           if a problem occurs
   */
  public List<String> getLastErrorModes() throws KettleException {
    List<String> customLastErrorModes = new ArrayList<String>();

    DB local = getDb( LOCAL_DB );
    if ( local != null ) {
      try {
        DBCollection replset = local.getCollection( REPL_SET_COLLECTION );
        if ( replset != null ) {
          DBObject config = replset.findOne();

          extractLastErrorModes( config, customLastErrorModes );
        }
      } catch ( Exception e ) {
        if ( e instanceof KettleException ) {
          throw (KettleException) e;
        } else {
          throw new KettleException( e );
        }
      }
    }

    return customLastErrorModes;
  }

  protected void extractLastErrorModes( DBObject config, List<String> customLastErrorModes ) {
    if ( config != null ) {
      Object settings = config.get( REPL_SET_SETTINGS );

      if ( settings != null ) {
        Object getLastErrModes = ( (DBObject) settings ).get( REPL_SET_LAST_ERROR_MODES );

        if ( getLastErrModes != null ) {
          for ( String m : ( (DBObject) getLastErrModes ).keySet() ) {
            customLastErrorModes.add( m );
          }
        }
      }
    }
  }

  public List<String> getIndexInfo( String dbName, String collection ) throws KettleException {
    try {
      DB db = getDb( dbName );

      if ( db == null ) {
        throw new Exception( BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.NonExistentDB", dbName ) ); //$NON-NLS-1$
      }

      if ( Const.isEmpty( collection ) ) {
        throw new Exception( BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
      }

      if ( !db.collectionExists( collection ) ) {
        db.createCollection( collection, null );
      }

      DBCollection coll = db.getCollection( collection );
      if ( coll == null ) {
        throw new Exception( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
            collection ) );
      }

      List<DBObject> collInfo = coll.getIndexInfo();
      List<String> result = new ArrayList<String>();
      if ( collInfo == null || collInfo.size() == 0 ) {
        throw new Exception( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToGetInfoForCollection", //$NON-NLS-1$
            collection ) );
      }

      for ( DBObject index : collInfo ) {
        result.add( index.toString() );
      }

      return result;
    } catch ( Exception e ) {
      log.logError( BaseMessages.getString( PKG, "MongoNoAuthWrapper.ErrorMessage.GeneralError.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getMessage(), e ); //$NON-NLS-1$
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e );
      }
    }
  }

  public List<MongoField> discoverFields( String db, String collection, String query, String fields,
      boolean isPipeline, int docsToSample ) throws KettleException {
    DBCursor cursor = null;
    try {
      int numDocsToSample = docsToSample;
      if ( numDocsToSample < 1 ) {
        numDocsToSample = 100; // default
      }

      List<MongoField> discoveredFields = new ArrayList<MongoField>();
      Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
      try {
        DB database = getDb( db );

        if ( Const.isEmpty( collection ) ) {
          throw new KettleException( BaseMessages.getString( PKG,
              "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
        }
        DBCollection dbcollection = database.getCollection( collection );

        Iterator<DBObject> pipeSample = null;

        if ( isPipeline ) {
          pipeSample = setUpPipelineSample( query, numDocsToSample, dbcollection );
        } else {
          if ( Const.isEmpty( query ) && Const.isEmpty( fields ) ) {
            cursor = dbcollection.find().limit( numDocsToSample );
          } else {
            DBObject dbObject = (DBObject) JSON.parse( Const.isEmpty( query ) ? "{}" //$NON-NLS-1$
                : query );
            DBObject dbObject2 = (DBObject) JSON.parse( fields );
            cursor = dbcollection.find( dbObject, dbObject2 ).limit( numDocsToSample );
          }
        }

        int actualCount = 0;
        while ( cursor != null ? cursor.hasNext() : pipeSample.hasNext() ) {
          actualCount++;
          DBObject nextDoc = ( cursor != null ? cursor.next() : pipeSample.next() );
          docToFields( nextDoc, fieldLookup );
        }

        postProcessPaths( fieldLookup, discoveredFields, actualCount );

        return discoveredFields;
      } catch ( Exception e ) {
        throw new KettleException( e );
      } finally {
        if ( cursor != null ) {
          cursor.close();
        }
      }
    } catch ( Exception ex ) {
      if ( ex instanceof KettleException ) {
        throw (KettleException) ex;
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields" ), ex ); //$NON-NLS-1$
      }
    }
  }

  private static Iterator<DBObject> setUpPipelineSample( String query, int numDocsToSample, DBCollection collection )
    throws KettleException {

    query = query + ", {$limit : " + numDocsToSample + "}"; //$NON-NLS-1$ //$NON-NLS-2$
    List<DBObject> samplePipe = jsonPipelineToDBObjectList( query );

    DBObject first = samplePipe.get( 0 );
    DBObject[] remainder = new DBObject[samplePipe.size() - 1];
    for ( int i = 1; i < samplePipe.size(); i++ ) {
      remainder[i - 1] = samplePipe.get( i );
    }

    AggregationOutput result = collection.aggregate( first, remainder );

    return result.results().iterator();
  }

  protected static List<DBObject> jsonPipelineToDBObjectList( String jsonPipeline ) throws KettleException {
    List<DBObject> pipeline = new ArrayList<DBObject>();
    StringBuilder b = new StringBuilder( jsonPipeline.trim() );

    // extract the parts of the pipeline
    int bracketCount = -1;
    List<String> parts = new ArrayList<String>();
    int i = 0;
    while ( i < b.length() ) {
      if ( b.charAt( i ) == '{' ) {
        if ( bracketCount == -1 ) {
          // trim anything off before this point
          b.delete( 0, i );
          bracketCount = 0;
          i = 0;
        }
        bracketCount++;
      }
      if ( b.charAt( i ) == '}' ) {
        bracketCount--;
      }
      if ( bracketCount == 0 ) {
        String part = b.substring( 0, i + 1 );
        parts.add( part );
        bracketCount = -1;

        if ( i == b.length() - 1 ) {
          break;
        }
        b.delete( 0, i + 1 );
        i = 0;
      }

      i++;
    }

    for ( String p : parts ) {
      if ( !Const.isEmpty( p ) ) {
        DBObject o = (DBObject) JSON.parse( p );
        pipeline.add( o );
      }
    }

    if ( pipeline.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "MongoNoAuthWrapper.ErrorMessage.UnableToParsePipelineOperators" ) ); //$NON-NLS-1$
    }

    return pipeline;
  }

  private void docToFields( DBObject doc, Map<String, MongoField> lookup ) {
    String root = "$"; //$NON-NLS-1$
    String name = "$"; //$NON-NLS-1$

    if ( doc instanceof BasicDBObject ) {
      processRecord( (BasicDBObject) doc, root, name, lookup );
    } else if ( doc instanceof BasicDBList ) {
      processList( (BasicDBList) doc, root, name, lookup );
    }
  }

  private void processRecord( BasicDBObject rec, String path, String name, Map<String, MongoField> lookup ) {
    for ( String key : rec.keySet() ) {
      Object fieldValue = rec.get( key );

      if ( fieldValue instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup );
      } else if ( fieldValue instanceof BasicDBList ) {
        processList( (BasicDBList) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
            + key, lookup );
      } else {
        // some sort of primitive
        String finalPath = path + "." + key; //$NON-NLS-1$
        String finalName = name + "." + key; //$NON-NLS-1$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( fieldValue );
          newField.m_mongoType = fieldValue;
          newField.m_fieldName = finalName;
          newField.m_fieldPath = finalPath;
          newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          if ( !m.m_mongoType.getClass().isAssignableFrom( fieldValue.getClass() ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  protected static int mongoToKettleType( Object fieldValue ) {
    if ( fieldValue == null ) {
      return ValueMetaInterface.TYPE_STRING;
    }

    if ( fieldValue instanceof Symbol || fieldValue instanceof String || fieldValue instanceof Code
        || fieldValue instanceof ObjectId || fieldValue instanceof MinKey || fieldValue instanceof MaxKey ) {
      return ValueMetaInterface.TYPE_STRING;
    } else if ( fieldValue instanceof Date ) {
      return ValueMetaInterface.TYPE_DATE;
    } else if ( fieldValue instanceof Number ) {
      // try to parse as an Integer
      try {
        Integer.parseInt( fieldValue.toString() );
        return ValueMetaInterface.TYPE_INTEGER;
      } catch ( NumberFormatException e ) {
        return ValueMetaInterface.TYPE_NUMBER;
      }
    } else if ( fieldValue instanceof Binary ) {
      return ValueMetaInterface.TYPE_BINARY;
    } else if ( fieldValue instanceof BSONTimestamp ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }

    return ValueMetaInterface.TYPE_STRING;
  }

  private void processList( BasicDBList list, String path, String name, Map<String, MongoField> lookup ) {

    if ( list.size() == 0 ) {
      return; // can't infer anything about an empty list
    }

    String nonPrimitivePath = path + "[-]"; //$NON-NLS-1$
    String primitivePath = path;

    for ( int i = 0; i < list.size(); i++ ) {
      Object element = list.get( i );

      if ( element instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
      } else if ( element instanceof BasicDBList ) {
        processList( (BasicDBList) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
            + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
      } else {
        // some sort of primitive
        String finalPath = primitivePath + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        String finalName = name + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( element );
          newField.m_mongoType = element;
          newField.m_fieldName = finalPath;
          newField.m_fieldPath = finalName;
          newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          if ( !m.m_mongoType.getClass().isAssignableFrom( element.getClass() ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  protected static void postProcessPaths( Map<String, MongoField> fieldLookup, List<MongoField> discoveredFields,
      int numDocsProcessed ) {
    for ( String key : fieldLookup.keySet() ) {
      MongoField m = fieldLookup.get( key );
      m.m_occurenceFraction = "" + m.m_percentageOfSample + "/" //$NON-NLS-1$ //$NON-NLS-2$
          + numDocsProcessed;
      setMinArrayIndexes( m );

      // set field names to terminal part and copy any min:max array index
      // info
      if ( m.m_fieldName.contains( "[" ) && m.m_fieldName.contains( ":" ) ) { //$NON-NLS-1$ //$NON-NLS-2$
        m.m_arrayIndexInfo = m.m_fieldName;
      }
      if ( m.m_fieldName.indexOf( '.' ) >= 0 ) {
        m.m_fieldName = m.m_fieldName.substring( m.m_fieldName.lastIndexOf( '.' ) + 1, m.m_fieldName.length() );
      }

      if ( m.m_disparateTypes ) {
        // force type to string if we've seen this path more than once
        // with incompatible types
        m.m_kettleType = ValueMeta.getTypeDesc( ValueMeta.TYPE_STRING );
      }
      discoveredFields.add( m );
    }

    // check for name clashes
    Map<String, Integer> tempM = new HashMap<String, Integer>();
    for ( MongoField m : discoveredFields ) {
      if ( tempM.get( m.m_fieldName ) != null ) {
        Integer toUse = tempM.get( m.m_fieldName );
        String key = m.m_fieldName;
        m.m_fieldName = key + "_" + toUse; //$NON-NLS-1$
        toUse = new Integer( toUse.intValue() + 1 );
        tempM.put( key, toUse );
      } else {
        tempM.put( m.m_fieldName, 1 );
      }
    }
  }

  protected static void setMinArrayIndexes( MongoField m ) {
    // set the actual index for each array in the path to the
    // corresponding minimum index
    // recorded in the name

    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    String temp = m.m_fieldPath;
    String tempComp = m.m_fieldName;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( !innerPart.equals( "-" ) ) { //$NON-NLS-1$
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1, temp.length() );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1, tempComp.length() );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        String replace = "[" + compParts[0] + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        updated.append( replace );

      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldPath = updated.toString();
  }

  protected static void updateMaxArrayIndexes( MongoField m, String update ) {
    // just look at the second (i.e. max index value) in the array parts
    // of update
    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    if ( m.m_fieldName.split( "\\[" ).length != update.split( "\\[" ).length ) { //$NON-NLS-1$ //$NON-NLS-2$
      throw new IllegalArgumentException( "Field path and update path do not seem to contain " //$NON-NLS-1$
          + "the same number of array parts!" ); //$NON-NLS-1$
    }

    String temp = m.m_fieldName;
    String tempComp = update;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( innerPart.indexOf( ':' ) < 0 ) {
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1, temp.length() );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1, tempComp.length() );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] origParts = innerPart.split( ":" ); //$NON-NLS-1$
        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        int origMax = Integer.parseInt( origParts[1] );
        int compMax = Integer.parseInt( compParts[1] );

        if ( compMax > origMax ) {
          // updated the max index seen for this path
          String newRange = "[" + origParts[0] + ":" + compMax + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
          updated.append( newRange );
        } else {
          String oldRange = "[" + innerPart + "]"; //$NON-NLS-1$ //$NON-NLS-2$
          updated.append( oldRange );
        }
      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldName = updated.toString();
  }

  @Override
  public List<String> getAllTags() throws KettleException {
    return setupAllTags( getRepSetMemberRecords() );
  }

  private BasicDBList getRepSetMemberRecords() throws KettleException {
    BasicDBList setMembers = null;
    try {
      DB local = getDb( LOCAL_DB );
      if ( local != null ) {

        DBCollection replset = local.getCollection( REPL_SET_COLLECTION );
        if ( replset != null ) {
          DBObject config = replset.findOne();

          if ( config != null ) {
            Object members = config.get( REPL_SET_MEMBERS );

            if ( members instanceof BasicDBList ) {
              if ( ( (BasicDBList) members ).size() == 0 ) {
                // log that there are no replica set members defined
                if ( log != null ) {
                  log.logBasic( BaseMessages.getString( PKG,
                      "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
                }
              } else {
                setMembers = (BasicDBList) members;
              }

            } else {
              // log that there are no replica set members defined
              if ( log != null ) {
                log.logBasic( BaseMessages.getString( PKG,
                    "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
              }
            }
          } else {
            // log that there are no replica set members defined
            if ( log != null ) {
              log.logBasic( BaseMessages.getString( PKG,
                  "MongoNoAuthWrapper.Message.Warning.NoReplicaSetMembersDefined" ) ); //$NON-NLS-1$
            }
          }
        } else {
          // log that the replica set collection is not available
          if ( log != null ) {
            log.logBasic( BaseMessages.getString( PKG,
                "MongoNoAuthWrapper.Message.Warning.ReplicaSetCollectionUnavailable" ) ); //$NON-NLS-1$
          }
        }
      } else {
        // log that the local database is not available!!
        if ( log != null ) {
          log.logBasic( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Warning.LocalDBNotAvailable" ) ); //$NON-NLS-1$
        }
      }
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    } finally {
      if ( getMongo() != null ) {
        getMongo().close();
      }
    }

    return setMembers;
  }

  protected List<String> setupAllTags( BasicDBList members ) {
    HashSet<String> tempTags = new HashSet<String>();

    if ( members != null && members.size() > 0 ) {
      for ( int i = 0; i < members.size(); i++ ) {
        Object m = members.get( i );

        if ( m != null ) {
          DBObject tags = (DBObject) ( (DBObject) m ).get( "tags" ); //$NON-NLS-1$
          if ( tags == null ) {
            continue;
          }

          for ( String tagName : tags.keySet() ) {
            String tagVal = tags.get( tagName ).toString();
            String combined = quote( tagName ) + " : " + quote( tagVal ); //$NON-NLS-1$
            tempTags.add( combined );
          }
        }
      }
    }

    return new ArrayList<String>( tempTags );
  }

  protected static String quote( String string ) {
    if ( string.indexOf( '"' ) >= 0 ) {

      if ( string.indexOf( '"' ) >= 0 ) {
        string = string.replace( "\"", "\\\"" ); //$NON-NLS-1$ //$NON-NLS-2$
      }
    }

    string = ( "\"" + string + "\"" ); //$NON-NLS-1$ //$NON-NLS-2$

    return string;
  }

  /**
   * Return a list of replica set members whos tags satisfy the supplied list of tag set. It is assumed that members
   * satisfy according to an OR relationship = i.e. a member satisfies if it satisfies at least one of the tag sets in
   * the supplied list.
   * 
   * @param tagSets
   *          the list of tag sets to match against
   * @return a list of replica set members who's tags satisfy the supplied list of tag sets
   * @throws KettleException
   *           if a problem occurs
   */
  public List<String> getReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets ) throws KettleException {
    try {
      List<String> result = new ArrayList<String>();
      for ( DBObject object : checkForReplicaSetMembersThatSatisfyTagSets( tagSets, getRepSetMemberRecords() ) ) {
        result.add( object.toString() );
      }
      return result;
    } catch ( Exception ex ) {
      if ( ex instanceof KettleException ) {
        throw (KettleException) ex;
      } else {
        throw new KettleException( BaseMessages.getString( PKG,
            "MongoNoAuthWrapper.ErrorMessage.UnableToGetReplicaSetMembers" ), ex ); //$NON-NLS-1$
      }
    }
  }

  protected List<DBObject> checkForReplicaSetMembersThatSatisfyTagSets( List<DBObject> tagSets, BasicDBList members ) {
    List<DBObject> satisfy = new ArrayList<DBObject>();
    if ( members != null && members.size() > 0 ) {
      for ( int i = 0; i < members.size(); i++ ) {
        Object m = members.get( i );

        if ( m != null ) {
          DBObject tags = (DBObject) ( (DBObject) m ).get( "tags" ); //$NON-NLS-1$
          if ( tags == null ) {
            continue;
          }

          for ( int j = 0; j < tagSets.size(); j++ ) {
            boolean match = true;
            DBObject toMatch = tagSets.get( j );

            for ( String tagName : toMatch.keySet() ) {
              String tagValue = toMatch.get( tagName ).toString();

              // does replica set member m's tags contain this tag?
              Object matchVal = tags.get( tagName );

              if ( matchVal == null ) {
                match = false; // doesn't match this particular tag set
                // no need to check any other keys in toMatch
                break;
              }

              if ( !matchVal.toString().equals( tagValue ) ) {
                // rep set member m's tags has this tag, but it's value does not
                // match
                match = false;

                // no need to check any other keys in toMatch
                break;
              }
            }

            if ( match ) {
              // all tag/values present and match - add this member (only if its
              // not already there)
              if ( !satisfy.contains( m ) ) {
                satisfy.add( (DBObject) m );
              }
            }
          }
        }
      }
    }

    return satisfy;
  }

  @Override
  public MongoCollectionWrapper getCollection( String db, String name ) throws KettleException {
    return wrap( getDb( db ).getCollection( name ) );
  }

  @Override
  public MongoCollectionWrapper createCollection( String db, String name ) throws KettleException {
    return wrap( getDb( db ).createCollection( name, null ) );
  }

  protected MongoCollectionWrapper wrap( DBCollection collection ) {
    return new DefaultMongoCollectionWrapper( collection );
  }

  @Override
  public void dispose() {
    getMongo().close();
  }
}
