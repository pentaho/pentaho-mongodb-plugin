/*!
 * Copyright 2010 - 2016 Pentaho Corporation.  All rights reserved.
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

import java.util.Arrays;

/**
 * Created by bryan on 8/22/14.
 */
public class MongoWrapperUtilTest {
  private static final String SOCKET_TIMEOUT = "mongoDbSocketTimeout";
  private static final String CONNECTION_TIMEOUT = "mongoDbConnectionTimeout";

  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = Mockito.mock( MongoWrapperClientFactory.class );
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
  }

  @After public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }

  @Test public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = Mockito.mock( MongoDbMeta.class );
    VariableSpace variableSpace = Mockito.mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = Mockito.mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = Mockito.mock( MongoClientWrapper.class );
    Mockito.when( mockFactory.createMongoClientWrapper( 
	    Matchers.any( MongoProperties.class ), Matchers.any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    Assert.assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test public void testCreateCalledReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = Mockito.mock( MongoDbMeta.class );
    VariableSpace variableSpace = Mockito.mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = Mockito.mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = Mockito.mock( MongoClientWrapper.class );
    Mockito.when( mongoDbMeta.getReadPrefTagSets() ).thenReturn( Arrays.asList( "test", "test2" ) );
    Mockito.when( mockFactory.createMongoClientWrapper( 
	    Matchers.any( MongoProperties.class ), Matchers.any( KettleMongoUtilLogger.class ) ) )
        .thenReturn( wrapper );
    Assert.assertEquals( wrapper,
        MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test public void testCreatePropertiesBuilder() {
    MongoDbMeta input = new MongoDbInputMeta();
    setSocetAndConnectionTimeouts( input, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}" );

    MongoDbMeta output = new MongoDbInputMeta();
    setSocetAndConnectionTimeouts( output, "${" + CONNECTION_TIMEOUT + "}", "${" + SOCKET_TIMEOUT + "}" );

    VariableSpace vars = new Variables();
    vars.setVariable( CONNECTION_TIMEOUT, "200" );
    vars.setVariable( SOCKET_TIMEOUT, "500" );

    MongoProperties inProps = MongoWrapperUtil.createPropertiesBuilder( input, vars ).build();
    MongoProperties outProps = MongoWrapperUtil.createPropertiesBuilder( output, vars ).build();

    checkProps( inProps, "200", "500" );
    checkProps( outProps, "200", "500" );
  }

  private void setSocetAndConnectionTimeouts(MongoDbMeta meta, String connection, String session) {
    meta.setConnectTimeout( connection );
    meta.setSocketTimeout( session );
  }

  private void checkProps(MongoProperties props, String cTimeout, String sTimeout) {
    Assert.assertEquals( cTimeout , props.get( MongoProp.connectTimeout ) );
    Assert.assertEquals( sTimeout , props.get( MongoProp.socketTimeout ) );
  }
}
