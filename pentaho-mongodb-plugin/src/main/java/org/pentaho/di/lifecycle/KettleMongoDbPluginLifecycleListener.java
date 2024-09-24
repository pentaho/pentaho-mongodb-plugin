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

package org.pentaho.di.lifecycle;

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbConnectionAnalyzer;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputExternalResourceConsumer;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputStepAnalyzer;
import org.pentaho.platform.engine.core.system.PentahoSystem;

@LifecyclePlugin( id = "KettleMongoDbPlugin", name = "KettleMongoDbPlugin" )
public class KettleMongoDbPluginLifecycleListener implements LifecycleListener {

  @Override
  public void onStart( LifeEventHandler handler ) throws LifecycleException {
    MongoDbInputStepAnalyzer mongoDbInputStepAnalyzer = new MongoDbInputStepAnalyzer();
    MongoDbInputExternalResourceConsumer mongoDbInputExternalResourceConsumer = new MongoDbInputExternalResourceConsumer();
    MongoDbConnectionAnalyzer mongoDbConnectionAnalyzer = new MongoDbConnectionAnalyzer();
    mongoDbInputStepAnalyzer.setConnectionAnalyzer( mongoDbConnectionAnalyzer );
    mongoDbInputStepAnalyzer.setExternalResourceConsumer( mongoDbInputExternalResourceConsumer );

    PentahoSystem.registerObject( mongoDbInputStepAnalyzer );
    PentahoSystem.registerObject( mongoDbInputExternalResourceConsumer );
    PentahoSystem.registerObject( mongoDbConnectionAnalyzer );
  }

  @Override public void onExit( LifeEventHandler handler ) throws LifecycleException {
    // no-op
  }
}
