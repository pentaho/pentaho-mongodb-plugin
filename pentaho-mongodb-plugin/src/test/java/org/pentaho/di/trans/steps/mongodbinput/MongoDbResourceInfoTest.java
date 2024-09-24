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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoDbResourceInfoTest {

  MongoDbResourceInfo info;

  MongoDbMeta meta;

  @BeforeClass
  public static void init() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    Encr.init( "Kettle" );
  }

  @Before
  public void setUp() throws Exception {
    meta = mock( MongoDbMeta.class );
    when( meta.getDbName() ).thenReturn( null );
    when( meta.getPort() ).thenReturn( null );
    when( meta.getHostnames() ).thenReturn( null );
    when( meta.getAuthenticationUser() ).thenReturn( null );
    when( meta.getAuthenticationPassword() ).thenReturn( null );
    when( meta.getUseAllReplicaSetMembers() ).thenReturn( false );
    when( meta.getUseKerberosAuthentication() ).thenReturn( false );
    when( meta.getConnectTimeout() ).thenReturn( null );
    when( meta.getSocketTimeout() ).thenReturn( null );
    when( meta.getCollection() ).thenReturn( null );
    StepMeta stepMeta = new StepMeta();
    TransMeta transMeta = new TransMeta();

    stepMeta.setParentTransMeta( transMeta );
    VariableSpace variables = new Variables();

    when( meta.getParentStepMeta() ).thenReturn( stepMeta );
    info = new MongoDbResourceInfo( meta );
  }

  @Test
  public void testStringConstructor() {
    info = new MongoDbResourceInfo( "localhost, remote.pentaho.com" , "1000", "myDb" );
    assertEquals( "localhost, remote.pentaho.com", info.getHostNames() );
    assertEquals( "1000", info.getPort() );
    assertEquals( "myDb", info.getDatabase() );
  }

  @Test
  public void testGetConnectTimeout() throws Exception {
    assertNull( info.getConnectTimeout() );
    info.setConnectTimeout( "1000" );
    assertEquals( "1000", info.getConnectTimeout() );
  }

  @Test
  public void testGetDatabase() throws Exception {
    assertNull( info.getDatabase() );
    info.setDatabase( "myDb" );
    assertEquals( "myDb", info.getDatabase() );
  }

  @Test
  public void testGetHostNames() throws Exception {
    assertNull( info.getHostNames() );
    info.setHostNames( "localhost, remote.pentaho.com" );
    assertEquals( "localhost, remote.pentaho.com", info.getHostNames() );
  }

  @Test
  public void testGetPort() throws Exception {
    assertNull( info.getPort() );
    info.setPort( "1000" );
    assertEquals( "1000", info.getPort() );
  }

  @Test
  public void testGetSocketTimeout() throws Exception {
    assertNull( info.getSocketTimeout() );
    info.setSocketTimeout( "1000" );
    assertEquals( "1000", info.getSocketTimeout() );
  }

  @Test
  public void testIsUseAllReplicaSetMembers() throws Exception {
    assertFalse( info.isUseAllReplicaSetMembers() );
    info.setUseAllReplicaSetMembers( true );
    assertTrue( info.isUseAllReplicaSetMembers() );
  }

  @Test
  public void testIsUseKerberosAuthentication() throws Exception {
    assertFalse( info.isUseKerberosAuthentication() );
    info.setUseKerberosAuthentication( true );
    assertTrue( info.isUseKerberosAuthentication() );
  }

  @Test
  public void testGetUser() throws Exception {
    assertNull( info.getUser() );
    info.setUser( "joe" );
    assertEquals( "joe", info.getUser() );
  }

  @Test
  public void testGetPassword() throws Exception {
    assertNull( info.getPassword() );
    info.setPassword( "password" );
    assertEquals( "password", info.getPassword() );
  }

  @Test
  public void testGetEncryptedPassword() throws Exception {
    assertNull( info.getPassword() );
    info.setPassword( "password" );
    assertEquals( "Encrypted 2be98afc86aa7f2e4bb18bd63c99dbdde", info.getEncryptedPassword() );
  }
}
