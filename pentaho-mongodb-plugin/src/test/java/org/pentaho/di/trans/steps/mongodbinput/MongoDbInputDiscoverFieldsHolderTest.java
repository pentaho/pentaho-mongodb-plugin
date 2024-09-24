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

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class MongoDbInputDiscoverFieldsHolderTest {

  @Mock MongoDbInputDiscoverFields discoverFields;
  @Mock MongoDbInputDiscoverFields discoverFields2;
  @Mock MongoDbInputDiscoverFields discoverFields3b;
  @Mock MongoDbInputDiscoverFields discoverFields3;
  MongoDbInputDiscoverFieldsHolder holder =  MongoDbInputDiscoverFieldsHolder.getInstance();

  @Before public void before() {
    MockitoAnnotations.initMocks( this );
  }

  @Test public void testImplAddedRemoved() throws Exception {
    holder.implAdded( discoverFields, ImmutableMap.of( "service.ranking", 1 ) );
    holder.implAdded( discoverFields2, Collections.emptyMap() );
    holder.implAdded( discoverFields3, ImmutableMap.of( "service.ranking", 5 ) );
    holder.implAdded( discoverFields3b, ImmutableMap.of( "service.ranking", 5 ) ); // second impl at same rank

    assertThat( holder.getMongoDbInputDiscoverFields(), equalTo( discoverFields3 ) );
    holder.implRemoved( discoverFields3, ImmutableMap.of( "service.ranking", 5 ) );
    assertThat( holder.getMongoDbInputDiscoverFields(), equalTo( discoverFields3b ) );
    holder.implRemoved( discoverFields3b, ImmutableMap.of( "service.ranking", 5 ) );
    assertThat( holder.getMongoDbInputDiscoverFields(), equalTo( discoverFields ) );
    holder.implRemoved( discoverFields, ImmutableMap.of( "service.ranking", 1 ) );
    assertThat( holder.getMongoDbInputDiscoverFields(), equalTo( discoverFields2 ) );
  }

}
