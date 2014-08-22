package org.pentaho.mongo.wrapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.MongoDbException;
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
  private MongoWrapperClientFactory cachedFactory;
  private MongoWrapperClientFactory mockFactory;

  @Before
  public void setup() {
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    mockFactory = mock( MongoWrapperClientFactory.class );
    MongoWrapperUtil.setMongoWrapperClientFactory( mockFactory );
  }

  @After
  public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }

  @Test
  public void testCreateCalledNoReadPrefs() throws MongoDbException {
    MongoDbMeta mongoDbMeta = mock( MongoDbMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );

    MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    when( mockFactory.createMongoClientWrapper( any( MongoProperties.class ), any( KettleMongoUtilLogger.class ) ) )
      .thenReturn( wrapper );
    assertEquals( wrapper,
      MongoWrapperUtil.createMongoClientWrapper( mongoDbMeta, variableSpace, logChannelInterface ) );
  }

  @Test
  public void testCreateCalledReadPrefs() throws MongoDbException {
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
}
