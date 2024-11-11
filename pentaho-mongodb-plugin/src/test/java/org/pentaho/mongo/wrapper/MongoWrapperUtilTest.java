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


package org.pentaho.mongo.wrapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
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
import org.pentaho.mongo.MongoUtilLogger;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/22/14.
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class )
public class MongoWrapperUtilTest {
  private static final String SOCKET_TIMEOUT = "mongoDbSocketTimeout";
  private static final String CONNECTION_TIMEOUT = "mongoDbConnectionTimeout";
  private static final String PASSWORD = "password";

  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = mock( MongoWrapperClientFactory.class );
  }

  @After public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }

  @Test public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
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
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
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
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
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
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
    final String pass = "pass";
    testPropertiesBuilderForPassword( true, pass );
    testPropertiesBuilderForPassword( false, pass );
  }

  @Test public void testCreateConnStringMongoClientWrapperUtil() throws MongoDbException {
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    try ( MockedStatic<Encr> mockedEncr = mockStatic( Encr.class ) ) {
      mockedEncr.when( () -> Encr.decryptPasswordOptionallyEncrypted( anyString() ) ).thenReturn( "SimpleString" );
      when( mongoDbMeta.isUseConnectionString() ).thenReturn( true );
      when( mongoDbMeta.getConnectionString() ).thenReturn( "SimpleString" );
      MongoClientWrapper wrapper = mock( ConnectionStringMongoClientWrapper.class );
      doReturn( wrapper ).when( mockFactory ).createConnectionStringMongoClientWrapper( Mockito.<String>any(), Mockito.<KettleMongoUtilLogger>any() );
      assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
      verify( mongoDbMeta ).isUseConnectionString();
    }
  }

  @Test public void testCreateConnStringMongoClientWrapperFactory() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    doReturn( true ).when( mongoDbMeta ).isUseConnectionString();
    doReturn( "SimpleString" ).when( mongoDbMeta ).getConnectionString();
    try ( MockedStatic<Encr> mockedEncr = mockStatic( Encr.class );
          MockedStatic<MongoClientWrapperFactory> mockedWrapper = mockStatic( MongoClientWrapperFactory.class )) {
      MongoClientWrapper wrapper = mock( ConnectionStringMongoClientWrapper.class );
      mockedEncr.when( () -> Encr.decryptPasswordOptionallyEncrypted( any() ) ).thenReturn( "SimpleString" );
      mockedWrapper.when( () -> MongoClientWrapperFactory.createConnectionStringMongoClientWrapper( Mockito.<String>any(), Mockito.<MongoUtilLogger>any() ) ).thenReturn( wrapper );

      assertEquals( wrapper, MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
      verify( mongoDbMeta ).isUseConnectionString();
    }
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

  private void setSocetAndConnectionTimeouts( MongoDbMeta meta, String connection, String session ) {
    meta.setConnectTimeout( connection );
    meta.setSocketTimeout( session );
  }

  private void setPassword( MongoDbMeta meta, String password ) {
    meta.setAuthenticationPassword( password );
  }

  private void checkProps( MongoProperties props, String cTimeout, String sTimeout ) {
    assertEquals( cTimeout, props.get( MongoProp.connectTimeout ) );
    assertEquals( sTimeout, props.get( MongoProp.socketTimeout ) );
  }

  private void checkPass( MongoProperties props, String password ) {
    assertEquals( password, props.get( MongoProp.PASSWORD ) );
  }
}