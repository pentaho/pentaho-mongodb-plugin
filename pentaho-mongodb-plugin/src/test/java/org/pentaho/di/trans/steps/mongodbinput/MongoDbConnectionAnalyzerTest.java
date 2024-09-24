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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IMetaverseBuilder;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.IMetaverseObjectFactory;
import org.pentaho.metaverse.api.INamespace;
import org.pentaho.metaverse.api.MetaverseObjectFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: RFellows Date: 3/6/15
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class )
public class MongoDbConnectionAnalyzerTest {

  MongoDbConnectionAnalyzer analyzer;

  @Mock private IMetaverseBuilder mockBuilder;
  @Mock private MongoDbMeta mongoDbMeta;
  @Mock private IComponentDescriptor mockDescriptor;

  @Before
  public void setUp() throws Exception {
    IMetaverseObjectFactory factory = new MetaverseObjectFactory();
    when( mockBuilder.getMetaverseObjectFactory() ).thenReturn( factory );

    analyzer = new MongoDbConnectionAnalyzer();
    analyzer.setMetaverseBuilder( mockBuilder );

    when( mockDescriptor.getNamespace() ).thenReturn( mock( INamespace.class) );
    when( mockDescriptor.getContext() ).thenReturn( mock( IAnalysisContext.class ) );

    when( mongoDbMeta.getHostnames() ).thenReturn( "localhost" );
    when( mongoDbMeta.getDbName() ).thenReturn( "db" );
    when( mongoDbMeta.getAuthenticationUser() ).thenReturn( "user" );
    when( mongoDbMeta.getPort() ).thenReturn( "12345" );
    when( mongoDbMeta.isUseLegacyOptions() ).thenReturn( true );
  }

  @Test
  public void testAnalyze() throws Exception {
    IMetaverseNode node = analyzer.analyze( mockDescriptor, mongoDbMeta );
    assertNotNull( node );
    assertEquals( "localhost", node.getProperty( MongoDbConnectionAnalyzer.HOST_NAMES ) );
    assertEquals( "db", node.getProperty( MongoDbConnectionAnalyzer.DATABASE_NAME ) );
    assertEquals( "user", node.getProperty( DictionaryConst.PROPERTY_USER_NAME ) );
    assertEquals( "12345", node.getProperty( DictionaryConst.PROPERTY_PORT ) );

  }

  @Test
  public void testGetUsedConnections() throws Exception {
    List<MongoDbMeta> dbMetaList = analyzer.getUsedConnections( mongoDbMeta );
    assertEquals( 1, dbMetaList.size() );

    // should just return the same MongoDbMeta object in list form as the only entry
    assertEquals( mongoDbMeta, dbMetaList.get( 0 ) );
  }

  @Test
  public void testBuildComponentDescriptor() throws Exception {
    IComponentDescriptor dbDesc = analyzer.buildComponentDescriptor( mockDescriptor, mongoDbMeta );
    assertNotNull( dbDesc );
    assertEquals( "db", dbDesc.getName() );
  }
}
