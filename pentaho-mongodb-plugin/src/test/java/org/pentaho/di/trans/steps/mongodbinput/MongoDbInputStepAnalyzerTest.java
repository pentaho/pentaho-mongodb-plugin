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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.dictionary.DictionaryConst;
import org.pentaho.dictionary.MetaverseTransientNode;
import org.pentaho.metaverse.api.IAnalysisContext;
import org.pentaho.metaverse.api.IComponentDescriptor;
import org.pentaho.metaverse.api.IConnectionAnalyzer;
import org.pentaho.metaverse.api.IMetaverseBuilder;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.IMetaverseObjectFactory;
import org.pentaho.metaverse.api.INamespace;
import org.pentaho.metaverse.api.MetaverseComponentDescriptor;
import org.pentaho.metaverse.api.MetaverseObjectFactory;
import org.pentaho.metaverse.api.analyzer.kettle.step.ExternalResourceStepAnalyzer;
import org.pentaho.metaverse.api.analyzer.kettle.step.IClonableStepAnalyzer;
import org.pentaho.metaverse.api.model.IExternalResourceInfo;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * User: RFellows Date: 3/6/15
 */
@RunWith( MockitoJUnitRunner.StrictStubs.class )
public class MongoDbInputStepAnalyzerTest {
  MongoDbInputStepAnalyzer analyzer;

  @Mock IMetaverseNode node;

  @Mock
  private IMetaverseBuilder mockBuilder;
  @Mock
  private MongoDbInputMeta meta;
  @Mock
  private INamespace mockNamespace;
  @Mock
  private StepMeta parentStepMeta;
  @Mock
  private TransMeta mockTransMeta;

  IComponentDescriptor descriptor;

  @BeforeClass
  public static void init() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    Encr.init( "Kettle" );
  }

  @Before
  public void setUp() throws Exception {
    IMetaverseObjectFactory factory = new MetaverseObjectFactory();
    when( mockBuilder.getMetaverseObjectFactory() ).thenReturn( factory );
    analyzer = spy( new MongoDbInputStepAnalyzer() );
    analyzer.setConnectionAnalyzer( mock( IConnectionAnalyzer.class ) );
    analyzer.setMetaverseBuilder( mockBuilder );
    analyzer.setBaseStepMeta( meta );
    analyzer.setRootNode( node );
    analyzer.setParentTransMeta( mockTransMeta );
    analyzer.setParentStepMeta( parentStepMeta );

    descriptor = new MetaverseComponentDescriptor( "test", DictionaryConst.NODE_TYPE_TRANS_STEP, mockNamespace );
    analyzer.setDescriptor( descriptor );

    when( meta.getParentStepMeta() ).thenReturn( parentStepMeta );

  }

  @Test
  public void testCustomAnalyze() throws Exception {
    when( meta.getJsonQuery() ).thenReturn( "{test:test}" );
    when( meta.getCollection() ).thenReturn( "myCollection" );
    when( meta.getQueryIsPipeline() ).thenReturn( true );
    IMetaverseNode node = new MetaverseTransientNode( "new node" );
    analyzer.customAnalyze( meta, node );
    assertNotNull( node );

    assertEquals( "{test:test}", node.getProperty( DictionaryConst.PROPERTY_QUERY ) );
    assertEquals( "myCollection", node.getProperty( MongoDbInputStepAnalyzer.COLLECTION ) );
    assertTrue( (Boolean) node.getProperty( MongoDbInputStepAnalyzer.AGG_PIPELINE ) );
  }

  @Test
  public void testCustomAnalyze_jsonOutput() throws Exception {
    when( meta.getOutputJson() ).thenReturn( true );
    when( meta.getCollection() ).thenReturn( "myCollection" );

    IMetaverseNode node = new MetaverseTransientNode( "new node" );
    analyzer.customAnalyze( meta, node );
    assertNotNull( node );

    assertEquals( "myCollection", node.getProperty( MongoDbInputStepAnalyzer.COLLECTION ) );
    assertTrue( (Boolean) node.getProperty( MongoDbInputStepAnalyzer.OUTPUT_JSON ) );
    assertNull( node.getProperty( DictionaryConst.PROPERTY_QUERY ) );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.AGG_PIPELINE ) );
  }

  @Test
  public void testCreateTableNode() throws Exception {
    IConnectionAnalyzer connectionAnalyzer = mock( IConnectionAnalyzer.class );

    IMetaverseNode connNode = mock( IMetaverseNode.class );

    MongoDbResourceInfo resourceInfo = mock( MongoDbResourceInfo.class );
    when( resourceInfo.getCollection() ).thenReturn( "myCollection" );

    IMetaverseNode connectionNode = mock( IMetaverseNode.class );
    doReturn( connectionNode ).when( analyzer ).getConnectionNode();
    when( connectionNode.getLogicalId() ).thenReturn( "CONNECTION_ID" );

    IMetaverseNode resourceNode = analyzer.createTableNode( resourceInfo );
    assertEquals( "myCollection", resourceNode.getProperty( MongoDbInputStepAnalyzer.COLLECTION ) );
    assertEquals( "myCollection", resourceNode.getName() );
    assertEquals( "CONNECTION_ID", resourceNode.getProperty( DictionaryConst.PROPERTY_NAMESPACE ) );
  }

  @Test
  public void testCreateOutputFieldNode() throws Exception {

    IAnalysisContext context = mock( IAnalysisContext.class );
    ValueMetaInterface vmi = new ValueMeta( "field1" );
    MongoField mongoField1 = new MongoField();
    mongoField1.m_fieldName = "field1";
    mongoField1.m_fieldPath = "$.field1";
    mongoField1.m_arrayIndexInfo = "range";
    mongoField1.m_occurenceFraction = "occurence";
    mongoField1.m_indexedVals = Arrays.asList( new String[] { "one", "two" } );
    mongoField1.m_disparateTypes = true;
    mongoField1.m_kettleType = "ValueMetaString";
    mongoField1.m_outputIndex = 0;
    List<MongoField> mongoFields = Arrays.asList( mongoField1 );
    when( meta.getMongoFields() ).thenReturn( mongoFields );

    when( node.getLogicalId() ).thenReturn( "logical id" );

    IMetaverseNode node = analyzer.createOutputFieldNode(
      context,
      vmi,
      ExternalResourceStepAnalyzer.RESOURCE,
      DictionaryConst.NODE_TYPE_TRANS_FIELD );

    assertNotNull( node );
    assertEquals( "field1", node.getName() );
    assertEquals( mongoField1.m_fieldPath, node.getProperty( MongoDbInputStepAnalyzer.JSON_PATH ) );
    assertEquals( mongoField1.m_arrayIndexInfo, node.getProperty( MongoDbInputStepAnalyzer.MINMAX_RANGE ) );
    assertEquals( mongoField1.m_occurenceFraction, node.getProperty( MongoDbInputStepAnalyzer.OCCUR_RATIO ) );
    assertEquals( mongoField1.m_indexedVals, node.getProperty( MongoDbInputStepAnalyzer.INDEXED_VALS ) );
    assertEquals( mongoField1.m_disparateTypes, node.getProperty( MongoDbInputStepAnalyzer.DISPARATE_TYPES ) );

  }

  @Test
  public void testCreateOutputFieldNode_noFields() throws Exception {

    ValueMetaInterface vmi = new ValueMeta( "field1" );
    IAnalysisContext context = mock( IAnalysisContext.class );
    when( meta.getMongoFields() ).thenReturn( null );

    when( node.getLogicalId() ).thenReturn( "logical id" );

    IMetaverseNode node = analyzer.createOutputFieldNode(
      context,
      vmi,
      ExternalResourceStepAnalyzer.RESOURCE,
      DictionaryConst.NODE_TYPE_TRANS_FIELD );

    assertEquals( "field1", node.getName() );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.JSON_PATH ) );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.MINMAX_RANGE ) );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.OCCUR_RATIO ) );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.INDEXED_VALS ) );
    assertNull( node.getProperty( MongoDbInputStepAnalyzer.DISPARATE_TYPES ) );


  }

  @Test
  public void testMongoDbInputExternalResourceConsumer() throws Exception {
    MongoDbInputExternalResourceConsumer consumer = new MongoDbInputExternalResourceConsumer();

    StepMeta meta = new StepMeta( "test", this.meta );
    StepMeta spyMeta = spy( meta );

    when( this.meta.getParentStepMeta() ).thenReturn( spyMeta );
    when( spyMeta.getParentTransMeta() ).thenReturn( mockTransMeta );

    assertFalse( consumer.isDataDriven( this.meta ) );
    Collection<IExternalResourceInfo> resources = consumer.getResourcesFromMeta( this.meta );
    assertNotNull( resources );
    assertEquals( 1, resources.size() );

    assertEquals( MongoDbInputMeta.class, consumer.getMetaClass() );
  }

  @Test
  public void testGetSupportedSteps() {
    Set<Class<? extends BaseStepMeta>> types = analyzer.getSupportedSteps();
    assertNotNull( types );
    assertEquals( types.size(), 1 );
    assertTrue( types.contains( MongoDbInputMeta.class ) );
  }

  @Test
  public void testGetResourceInputNodeType() throws Exception {
    assertEquals( DictionaryConst.NODE_TYPE_DATA_COLUMN, analyzer.getResourceInputNodeType() );
  }

  @Test
  public void testGetResourceOutputNodeType() throws Exception {
    assertNull( analyzer.getResourceOutputNodeType() );
  }

  @Test
  public void testIsOutput() throws Exception {
    assertFalse( analyzer.isOutput() );
  }

  @Test
  public void testIsInput() throws Exception {
    assertTrue( analyzer.isInput() );
  }

  @Test
  public void testCloneAnalyzer() {
    final IClonableStepAnalyzer analyzer = new MongoDbInputStepAnalyzer();
    // verify that cloneAnalyzer returns an instance that is different from the original
    Assert.assertNotEquals( analyzer, analyzer.cloneAnalyzer() );
  }
}

