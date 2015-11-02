/*!
 * Copyright 2010 - 2015 Pentaho Corporation.  All rights reserved.
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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pentaho.di.core.logging.LogChannelInterface;

import static org.junit.Assert.*;
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
