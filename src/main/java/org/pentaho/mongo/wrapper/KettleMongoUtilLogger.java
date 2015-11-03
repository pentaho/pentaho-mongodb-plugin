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

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/7/14.
 */
public class KettleMongoUtilLogger implements MongoUtilLogger {
  private final LogChannelInterface logChannelInterface;

  public KettleMongoUtilLogger( LogChannelInterface logChannelInterface ) {
    this.logChannelInterface = logChannelInterface;
  }

  @Override public void debug( String s ) {
    if ( logChannelInterface != null ) {
      logChannelInterface.logDebug( s );
    }
  }

  @Override public void info( String s ) {
    if ( logChannelInterface != null ) {
      logChannelInterface.logBasic( s );
    }
  }

  @Override public void warn( String s, Throwable throwable ) {
    if ( logChannelInterface != null ) {
      logChannelInterface.logError( s, throwable );
    }
  }

  @Override public void error( String s, Throwable throwable ) {
    if ( logChannelInterface != null ) {
      logChannelInterface.logError( s, throwable );
    }
  }

  @Override public boolean isDebugEnabled() {
    if ( logChannelInterface != null ) {
      return logChannelInterface.isDebug();
    } else {
      return false;
    }
  }
}
