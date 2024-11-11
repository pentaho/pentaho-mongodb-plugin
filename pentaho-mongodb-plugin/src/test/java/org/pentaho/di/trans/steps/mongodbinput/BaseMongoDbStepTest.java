/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.Cursor;
import com.mongodb.DBObject;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LogChannelInterfaceFactory;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.MongoUtilLogger;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperClientFactory;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

/**
 * Common mock setup for MongoDbOutputTest and MongoDbInput
 */
public class BaseMongoDbStepTest {

  @Mock protected StepMeta stepMeta;
  @Mock protected TransMeta transMeta;
  @Mock protected Trans trans;
  @Mock protected LogChannelInterface mockLog;
  @Mock protected MongoWrapperClientFactory mongoClientWrapperFactory;
  @Mock protected MongoClientWrapper mongoClientWrapper;
  @Mock protected MongoCollectionWrapper mongoCollectionWrapper;
  @Mock protected LogChannelInterfaceFactory logChannelFactory;
  @Mock protected Cursor cursor;
  @Captor protected ArgumentCaptor<String> stringCaptor;
  @Captor protected ArgumentCaptor<DBObject> dbObjectCaptor;
  @Captor protected ArgumentCaptor<Throwable> throwableCaptor;

  protected RowMeta rowMeta = new RowMeta();
  protected Object[] rowData;

  private MongoWrapperClientFactory cachedFactory;

  @Before public void before() throws MongoDbException {
    MockitoAnnotations.openMocks( BaseMongoDbStepTest.class );
    cachedFactory = MongoWrapperUtil.getMongoWrapperClientFactory();
    MongoWrapperUtil.setMongoWrapperClientFactory( mongoClientWrapperFactory );
    when( mongoClientWrapperFactory
        .createMongoClientWrapper( Mockito.<MongoProperties>any(), Mockito.<MongoUtilLogger>any() ) )
        .thenReturn( mongoClientWrapper );

    when( stepMeta.getName() ).thenReturn( "stepMetaName" );
    when( transMeta.findStep( anyString() ) ).thenReturn( stepMeta );
    when( logChannelFactory.create( any( BaseStep.class ), any( Trans.class ) ) ).thenReturn( mockLog );
    KettleLogStore.setLogChannelInterfaceFactory( logChannelFactory );
  }

  @After public void tearDown() {
    MongoWrapperUtil.setMongoWrapperClientFactory( cachedFactory );
  }
}
