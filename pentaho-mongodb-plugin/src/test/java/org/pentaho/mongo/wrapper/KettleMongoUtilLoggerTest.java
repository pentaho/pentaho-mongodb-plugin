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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.logging.LogChannelInterface;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by matt on 10/30/15.
 */
public class KettleMongoUtilLoggerTest {

  @Mock LogChannelInterface logChannelInterface;
  @Mock Exception exception;
  KettleMongoUtilLogger logger;

  @Before public void before() {
    MockitoAnnotations.initMocks( this );
    logger = new KettleMongoUtilLogger( logChannelInterface );
  }

  @Test public void testLoggingDelegates() throws Exception {
    logger.debug( "log" );
    verify( logChannelInterface ).logDebug( "log" );
    logger.info( "log" );
    verify( logChannelInterface ).logBasic( "log" );
    logger.warn( "log", exception );
    logger.error( "log", exception );
    // both warn and error are mapped to logError.
    verify( logChannelInterface, times( 2 ) ).logError( "log", exception );
    logger.isDebugEnabled();
    verify( logChannelInterface ).isDebug();
  }

}
