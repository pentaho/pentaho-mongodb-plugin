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

import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.analyzer.kettle.step.BaseStepExternalResourceConsumer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class MongoDbInputExternalResourceConsumer
  extends BaseStepExternalResourceConsumer<MongoDbInput, MongoDbInputMeta> {

  @Override
  public Class<MongoDbInputMeta> getMetaClass() {
    return MongoDbInputMeta.class;
  }

  @Override
  public Collection<IExternalResourceInfo> getResourcesFromMeta( MongoDbInputMeta meta, IAnalysisContext context ) {
    Set<IExternalResourceInfo> resources = new HashSet<IExternalResourceInfo>();
    MongoDbResourceInfo mongoDbResourceInfo = new MongoDbResourceInfo( meta );
    mongoDbResourceInfo.setInput( true );
    resources.add( mongoDbResourceInfo );
    return resources;
  }
}
