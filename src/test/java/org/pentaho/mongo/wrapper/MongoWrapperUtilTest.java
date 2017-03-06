/*!
 * Copyright 2010 - 2017 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.mongo.wrapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.trans.steps.mongodboutput.MongoDbOutputMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/22/14.
 */
public class MongoWrapperUtilTest {
  private static final String SOCKET_TIMEOUT = "mongoDbSocketTimeout";
  private static final String CONNECTION_TIMEOUT = "mongoDbConnectionTimeout";
  private static final String PASSWORD = "password";

  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = mock( MongoWrapperClientFactory.class );
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
  }

  @After public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }

  @Test public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    when( mockFactory.createMongoClientWrapper( any( MongoProperties.class ), any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test public void testCreateCalledReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    when( mongoDbMeta.getReadPrefTagSets() ).thenReturn( Arrays.asList( "test", "test2" ) );
    when( mockFactory.createMongoClientWrapper( any( MongoProperties.class ), any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test public void testCreatePropertiesBuilder() {
    MongoDbMeta input = new MongoDbInputMeta();
    setSocetAndConnectionTimeouts( input, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}" );

    MongoDbMeta output = new MongoDbOutputMeta();
    setSocetAndConnectionTimeouts( output, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}" );

    VariableSpace vars = new Variables();
    vars.setVariable( CONNECTION_TIMEOUT, "200" );
    vars.setVariable( SOCKET_TIMEOUT, "500" );

    MongoProperties inProps = MongoWrapperUtil.createPropertiesBuilder( input, vars ).build();
    MongoProperties outProps = MongoWrapperUtil.createPropertiesBuilder( output, vars ).build();

    checkProps( inProps, "200", "500" );
    checkProps( outProps, "200", "500" );
  }

  @Test public void testPropertiesBuilderEncrPassword() throws KettleException {
    final String pass = "pass";
    testPropertiesBuilderForPassword( true, pass );
    testPropertiesBuilderForPassword( false, pass );
  }

  private void testPropertiesBuilderForPassword( boolean isEncrypted, String password ) throws KettleException {
    MongoDbMeta input = new MongoDbInputMeta();
    setPassword( input, "${" + PASSWORD + "}" );

    MongoDbMeta output = new MongoDbOutputMeta();
    setPassword( output, "${" + PASSWORD + "}" );

    VariableSpace vars = new Variables();

    initEncryptor();

    String value;
    if ( isEncrypted ) {
      value = Encr.encryptPasswordIfNotUsingVariables( password );
    } else {
      value = password;
    }
    vars.setVariable( PASSWORD, value );

    MongoProperties inProps = MongoWrapperUtil.createPropertiesBuilder( input, vars ).build();
    MongoProperties outProps = MongoWrapperUtil.createPropertiesBuilder( output, vars ).build();

    checkPass( inProps, password );
    checkPass( outProps, password );
  }

  private void initEncryptor() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( true );
    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  private void setSocetAndConnectionTimeouts(MongoDbMeta meta, String connection, String session) {
    meta.setConnectTimeout( connection );
    meta.setSocketTimeout( session );
  }

  private void setPassword( MongoDbMeta meta, String password ) {
    meta.setAuthenticationPassword( password );
  }

  private void checkProps( MongoProperties props, String cTimeout, String sTimeout ) {
    assertEquals( cTimeout , props.get( MongoProp.connectTimeout ) );
    assertEquals( sTimeout , props.get( MongoProp.socketTimeout ) );
  }

  private void checkPass( MongoProperties props, String password ) {
    assertEquals( password, props.get( MongoProp.PASSWORD ) );
  }
}