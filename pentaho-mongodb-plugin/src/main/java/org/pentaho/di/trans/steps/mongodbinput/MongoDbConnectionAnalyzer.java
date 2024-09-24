/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IConnectionAnalyzer;
import org.pentaho.metaverse.api.ILogicalIdGenerator;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.MetaverseLogicalIdGenerator;
import org.pentaho.metaverse.api.analyzer.kettle.BaseKettleMetaverseComponent;

import java.util.ArrayList;
import java.util.List;

/**
 * User: RFellows Date: 3/6/15
 */
public class MongoDbConnectionAnalyzer extends BaseKettleMetaverseComponent implements
  IConnectionAnalyzer<MongoDbMeta, MongoDbMeta> {

  public static final String HOST_NAMES = "hostNames";
  public static final String DATABASE_NAME = "databaseName";
  public static final String USE_ALL_REPLICA_SET_MEMBERS = "useAllReplicaSetMembers";
  public static final String USE_KERBEROS_AUTHENTICATION = "useKerberosAuthentication";
  public static final String CONNECTION_TIMEOUT = "connectionTimeout";
  public static final String SOCKET_TIMEOUT = "socketTimeout";
  public static final String CONNECTION_STRING = "connectionString";

  public static final ILogicalIdGenerator ID_GENERATOR = new MetaverseLogicalIdGenerator(
      HOST_NAMES,
      DATABASE_NAME,
      DictionaryConst.PROPERTY_PORT,
      DictionaryConst.PROPERTY_USER_NAME
  );

  @Override
  public IMetaverseNode analyze( IComponentDescriptor descriptor, MongoDbMeta mongoDbMeta )
    throws MetaverseAnalyzerException {

    IMetaverseNode datasourceNode = createNodeFromDescriptor( descriptor );

    String database = mongoDbMeta.getDbName();
    datasourceNode.setName( database );
    datasourceNode.setProperty( DATABASE_NAME, database );
    datasourceNode.setType( DictionaryConst.NODE_TYPE_MONGODB_CONNECTION );
    datasourceNode.setLogicalIdGenerator( getLogicalIdGenerator() );

    if ( mongoDbMeta.isUseLegacyOptions() ) {
      String port = mongoDbMeta.getPort();
      String hostNames = mongoDbMeta.getHostnames();
      String user = mongoDbMeta.getAuthenticationUser();
      boolean useAllReplicaSetMembers = mongoDbMeta.getUseAllReplicaSetMembers();
      boolean useKerberosAuthentication = mongoDbMeta.getUseKerberosAuthentication();
      String connectTimeout = mongoDbMeta.getConnectTimeout();
      String socketTimeout = mongoDbMeta.getSocketTimeout();

      datasourceNode.setProperty( HOST_NAMES, hostNames );
      datasourceNode.setProperty( DictionaryConst.PROPERTY_USER_NAME, user );
      datasourceNode.setProperty( DictionaryConst.PROPERTY_PORT, port );
      datasourceNode.setProperty( USE_ALL_REPLICA_SET_MEMBERS, useAllReplicaSetMembers );
      datasourceNode.setProperty( USE_KERBEROS_AUTHENTICATION, useKerberosAuthentication );
      datasourceNode.setProperty( CONNECTION_TIMEOUT, connectTimeout );
      datasourceNode.setProperty( SOCKET_TIMEOUT, socketTimeout );
    } else if ( mongoDbMeta.isUseConnectionString() ) {
      String connectionString = mongoDbMeta.getConnectionString();
      datasourceNode.setProperty( CONNECTION_STRING, connectionString );
    }
    return datasourceNode;
  }

  @Override
  public List<MongoDbMeta> getUsedConnections( MongoDbMeta meta ) {
    List<MongoDbMeta> metas = new ArrayList<MongoDbMeta>();
    metas.add( meta );
    return metas;
  }

  @Override
  public IComponentDescriptor buildComponentDescriptor( IComponentDescriptor parentDescriptor,
                                                        MongoDbMeta connection ) {

    IComponentDescriptor dbDescriptor = new MetaverseComponentDescriptor(
        connection.getDbName(),
        DictionaryConst.NODE_TYPE_MONGODB_CONNECTION,
        parentDescriptor.getNamespace(),
        parentDescriptor.getContext() );

    return dbDescriptor;
  }

  @Override
  protected ILogicalIdGenerator getLogicalIdGenerator() {
    return ID_GENERATOR;
  }
}
