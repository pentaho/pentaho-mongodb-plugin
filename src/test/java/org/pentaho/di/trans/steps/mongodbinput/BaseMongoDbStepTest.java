/*!
 * PENTAHO CORPORATION PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2002 - 2015 Pentaho Corporation (Pentaho). All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Pentaho and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Pentaho and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Pentaho is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Pentaho,
 * explicitly covering such access.
 */

package org.pentaho.di.trans.steps.mongodbinput;

import com.mongodb.AggregationOutput;
import com.mongodb.DBObject;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
  @Mock protected AggregationOutput results;
  @Captor protected ArgumentCaptor<String> stringCaptor;
  @Captor protected ArgumentCaptor<DBObject> dbObjectCaptor;
  @Captor protected ArgumentCaptor<Throwable> throwableCaptor;

  protected RowMeta rowMeta = new RowMeta();
  protected Object[] rowData;

  @Before public void before() throws MongoDbException {
    MockitoAnnotations.initMocks( this );
    MongoWrapperUtil.setMongoWrapperClientFactory( mongoClientWrapperFactory );
    when( mongoClientWrapperFactory
        .createMongoClientWrapper( any( MongoProperties.class ), any( MongoUtilLogger.class ) ) )
        .thenReturn( mongoClientWrapper );

    when( stepMeta.getName() ).thenReturn( "stepMetaName" );
    when( transMeta.findStep( anyString() ) ).thenReturn( stepMeta );
    when( logChannelFactory.create( any( BaseStep.class ), any( Trans.class ) ) ).thenReturn( mockLog );
    KettleLogStore.setLogChannelInterfaceFactory( logChannelFactory );
  }


}
