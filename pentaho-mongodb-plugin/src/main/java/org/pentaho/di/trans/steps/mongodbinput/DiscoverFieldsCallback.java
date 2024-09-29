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
package org.pentaho.di.trans.steps.mongodbinput;

import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 12/30/14.
 */
public interface DiscoverFieldsCallback {
  public void notifyFields( List<MongoField> fields );
  public void notifyException( Exception exception );
}
