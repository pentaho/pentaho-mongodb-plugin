/*!
 * Copyright 2010 - 2016 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.trans.steps.mongodboutput;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;

import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Dialog class for the MongoDB output step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class MongoDbOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = MongoDbOutputMeta.class;

  protected MongoDbOutputMeta m_currentMeta;
  protected MongoDbOutputMeta m_originalMeta;

  /**
   * various UI bits and pieces for the dialog
   */
  private Label m_stepnameLabel;
  private Text m_stepnameText;

  // The tabs of the dialog
  private CTabFolder m_wTabFolder;
  private CTabItem m_wConfigTab;
  private CTabItem m_wOutputOptionsTab;
  private CTabItem m_wMongoFieldsTab;
  private Button m_getFieldsBut;
  private Button m_previewDocStructBut;
  private CTabItem m_wMongoIndexesTab;
  private Button m_showIndexesBut;

  private TextVar m_hostnameField;
  private TextVar m_portField;
  private Button m_useAllReplicaSetMembersBut;
  private TextVar m_authDbName;
  private TextVar m_usernameField;
  private TextVar m_passField;
  private CCombo  m_dbAuthMec;

  private Button m_kerberosBut;

  private TextVar m_connectTimeout;
  private TextVar m_socketTimeout;

  private CCombo m_dbNameField;
  private Button m_getDBsBut;
  private CCombo m_collectionField;
  private Button m_getCollectionsBut;

  private TextVar m_batchInsertSizeField;

  private Button m_truncateBut;
  private Button m_updateBut;
  private Button m_upsertBut;
  private Button m_multiBut;
  private Button m_modifierUpdateBut;

  private CCombo m_writeConcern;
  private TextVar m_wTimeout;
  private Button m_journalWritesCheck;
  private CCombo m_readPreference;

  private TextVar m_writeRetries;
  private TextVar m_writeRetryDelay;

  private TableView m_mongoFieldsView;
  private TableView m_mongoIndexesView;

  public MongoDbOutputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );

    m_currentMeta = (MongoDbOutputMeta) in;
    m_originalMeta = (MongoDbOutputMeta) m_currentMeta.clone();
  }

  @Override public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, m_currentMeta );

    // used to listen to a text field (m_wStepname)
    ModifyListener lsMod = new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( getString( "MongoDbOutputDialog.Shell.Title" ) ); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    m_stepnameLabel = new Label( shell, SWT.RIGHT );
    m_stepnameLabel.setText( getString( "MongoDbOutputDialog.StepName.Label" ) ); //$NON-NLS-1$
    props.setLook( m_stepnameLabel );

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( 0, margin );
    m_stepnameLabel.setLayoutData( fd );
    m_stepnameText = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_stepnameText.setText( stepname );
    props.setLook( m_stepnameText );
    m_stepnameText.addModifyListener( lsMod );

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_stepnameText.setLayoutData( fd );

    m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );
    m_wTabFolder.setSimple( false );

    // Start of the config tab
    m_wConfigTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wConfigTab.setText( getString( "MongoDbOutputDialog.ConfigTab.TabTitle" ) ); //$NON-NLS-1$

    Composite wConfigComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wConfigComp );

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout( configLayout );

    // hostname line
    Label hostnameLab = new Label( wConfigComp, SWT.RIGHT );
    hostnameLab.setText( getString( "MongoDbOutputDialog.Hostname.Label" ) ); //$NON-NLS-1$
    hostnameLab.setToolTipText( getString( "MongoDbOutputDialog.Hostname.TipText" ) ); //$NON-NLS-1$
    props.setLook( hostnameLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( middle, -margin );
    hostnameLab.setLayoutData( fd );

    m_hostnameField = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_hostnameField );
    m_hostnameField.addModifyListener( lsMod );
    // set the tool tip to the contents with any env variables expanded
    m_hostnameField.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_hostnameField.setToolTipText( transMeta.environmentSubstitute( m_hostnameField.getText() ) );
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( middle, 0 );
    m_hostnameField.setLayoutData( fd );

    // port line
    Label portLab = new Label( wConfigComp, SWT.RIGHT );
    portLab.setText( getString( "MongoDbOutputDialog.Port.Label" ) ); //$NON-NLS-1$
    portLab.setToolTipText( getString( "MongoDbOutputDialog.Port.Label.TipText" ) ); //$NON-NLS-1$
    props.setLook( portLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_hostnameField, margin );
    fd.right = new FormAttachment( middle, -margin );
    portLab.setLayoutData( fd );

    m_portField = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_portField );
    m_portField.addModifyListener( lsMod );
    // set the tool tip to the contents with any env variables expanded
    m_portField.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_portField.setToolTipText( transMeta.environmentSubstitute( m_portField.getText() ) );
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_hostnameField, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_portField.setLayoutData( fd );

    // Use all replica set members check box
    Label useAllReplicaLab = new Label( wConfigComp, SWT.RIGHT );
    useAllReplicaLab
      .setText( getString( "MongoDbOutputDialog.UseAllReplicaSetMembers.Label" ) ); //$NON-NLS-1$
    useAllReplicaLab.setToolTipText( getString( "MongoDbOutputDialog.UseAllReplicaSetMembers.TipText" ) ); //$NON-NLS-1$
    props.setLook( useAllReplicaLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( m_portField, margin );
    useAllReplicaLab.setLayoutData( fd );

    m_useAllReplicaSetMembersBut = new Button( wConfigComp, SWT.CHECK );
    props.setLook( m_useAllReplicaSetMembersBut );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_portField, margin );
    m_useAllReplicaSetMembersBut.setLayoutData( fd );

    // authentication database field
    Label authBdLab = new Label( wConfigComp, SWT.RIGHT );
    authBdLab.setText( getString( "MongoDbOutputDialog.AuthenticationDatabaseName.Label" ) ); //$NON-NLS-1$
    props.setLook( authBdLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_useAllReplicaSetMembersBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    authBdLab.setLayoutData( fd );

    m_authDbName = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_authDbName );
    m_authDbName.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_useAllReplicaSetMembersBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_authDbName.setLayoutData( fd );

    // username field
    Label userLab = new Label( wConfigComp, SWT.RIGHT );
    userLab.setText( getString( "MongoDbOutputDialog.Username.Label" ) ); //$NON-NLS-1$
    props.setLook( userLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_authDbName, margin );
    fd.right = new FormAttachment( middle, -margin );
    userLab.setLayoutData( fd );

    m_usernameField = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_usernameField );
    m_usernameField.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_authDbName, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_usernameField.setLayoutData( fd );

    // password field
    Label passLab = new Label( wConfigComp, SWT.RIGHT );
    passLab.setText( getString( "MongoDbOutputDialog.Password.Label" ) ); //$NON-NLS-1$
    props.setLook( passLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_usernameField, margin );
    fd.right = new FormAttachment( middle, -margin );
    passLab.setLayoutData( fd );

    m_passField = new PasswordTextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_passField );
    m_passField.addModifyListener( lsMod );

    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_usernameField, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_passField.setLayoutData( fd );
    Control lastControl = m_passField;

    // Authentication Mechanisms
    Label wlAuthMec = new Label( wConfigComp, SWT.RIGHT );
    wlAuthMec.setText( getString( "MongoDbOutputDialog.AuthMechanism.Label" ) ); //$NON-NLS-1$
    props.setLook( wlAuthMec );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    wlAuthMec.setLayoutData( fd );

    m_dbAuthMec = new CCombo( wConfigComp, SWT.BORDER );
    props.setLook( m_dbAuthMec );
    m_dbAuthMec.addModifyListener( new ModifyListener() {
        @Override public void modifyText( ModifyEvent e ) {
          transMeta.setChanged();
          m_dbAuthMec.setToolTipText( m_dbAuthMec.getText()  );
        }
    } );
    m_dbAuthMec.add( "SCRAM-SHA-1" );
    m_dbAuthMec.add( "MONGODB-CR" );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_dbAuthMec.setLayoutData( fd );
    lastControl = m_dbAuthMec;


    // use kerberos authentication
    Label kerbLab = new Label( wConfigComp, SWT.RIGHT );
    kerbLab.setText( getString( "MongoDbOutputDialog.Kerberos.Label" ) ); //$NON-NLS-1$
    props.setLook( kerbLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    kerbLab.setLayoutData( fd );

    m_kerberosBut = new Button( wConfigComp, SWT.CHECK );
    props.setLook( m_kerberosBut );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    m_kerberosBut.setLayoutData( fd );

    m_kerberosBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_passField.setEnabled( !m_kerberosBut.getSelection() );
      }
    } );

    // connection timeout
    Label connectTimeoutL = new Label( wConfigComp, SWT.RIGHT );
    connectTimeoutL
      .setText( getString( "MongoDbOutputDialog.ConnectionTimeout.Label" ) ); //$NON-NLS-1$
    props.setLook( connectTimeoutL );
    connectTimeoutL.setToolTipText( getString( "MongoDbOutputDialog.ConnectionTimeout.TipText" ) ); //$NON-NLS-1$

    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( m_kerberosBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    connectTimeoutL.setLayoutData( fd );

    m_connectTimeout = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_connectTimeout );
    m_connectTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_kerberosBut, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_connectTimeout.setLayoutData( fd );
    m_connectTimeout.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_connectTimeout.setToolTipText( transMeta.environmentSubstitute( m_connectTimeout.getText() ) );
      }
    } );

    // socket timeout
    Label socketTimeoutL = new Label( wConfigComp, SWT.RIGHT );
    socketTimeoutL.setText( getString( "MongoDbOutputDialog.SocketTimeout.Label" ) ); //$NON-NLS-1$
    props.setLook( connectTimeoutL );
    socketTimeoutL.setToolTipText( getString( "MongoDbOutputDialog.SocketTimeout.TipText" ) ); //$NON-NLS-1$

    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( m_connectTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    socketTimeoutL.setLayoutData( fd );

    m_socketTimeout = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_socketTimeout );
    m_socketTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_connectTimeout, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_socketTimeout.setLayoutData( fd );
    m_socketTimeout.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_socketTimeout.setToolTipText( transMeta.environmentSubstitute( m_socketTimeout.getText() ) );
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wConfigComp.setLayoutData( fd );

    wConfigComp.layout();
    m_wConfigTab.setControl( wConfigComp );

    // --- start of the options tab
    m_wOutputOptionsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wOutputOptionsTab.setText( "Output options" ); //$NON-NLS-1$
    Composite wOutputComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wOutputComp );
    FormLayout outputLayout = new FormLayout();
    outputLayout.marginWidth = 3;
    outputLayout.marginHeight = 3;
    wOutputComp.setLayout( outputLayout );

    // DB name
    Label dbNameLab = new Label( wOutputComp, SWT.RIGHT );
    dbNameLab.setText( getString( "MongoDbOutputDialog.DBName.Label" ) ); //$NON-NLS-1$
    dbNameLab.setToolTipText( getString( "MongoDbOutputDialog.DBName.TipText" ) ); //$NON-NLS-1$
    props.setLook( dbNameLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( middle, -margin );
    dbNameLab.setLayoutData( fd );

    m_getDBsBut = new Button( wOutputComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getDBsBut );
    m_getDBsBut.setText( getString( "MongoDbOutputDialog.GetDBs.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    m_getDBsBut.setLayoutData( fd );

    m_dbNameField = new CCombo( wOutputComp, SWT.BORDER );
    props.setLook( m_dbNameField );

    m_dbNameField.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        m_dbNameField.setToolTipText( transMeta.environmentSubstitute( m_dbNameField.getText() ) );
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_passField, margin );
    fd.right = new FormAttachment( m_getDBsBut, -margin );
    m_dbNameField.setLayoutData( fd );

    m_getDBsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        setupDBNames();
      }
    } );

    // collection line
    Label collectionLab = new Label( wOutputComp, SWT.RIGHT );
    collectionLab.setText( getString( "MongoDbOutputDialog.Collection.Label" ) ); //$NON-NLS-1$
    collectionLab.setToolTipText( getString( "MongoDbOutputDialog.Collection.TipText" ) ); //$NON-NLS-1$
    props.setLook( collectionLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_dbNameField, margin );
    fd.right = new FormAttachment( middle, -margin );
    collectionLab.setLayoutData( fd );

    m_getCollectionsBut = new Button( wOutputComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getCollectionsBut );
    m_getCollectionsBut.setText( getString( "MongoDbOutputDialog.GetCollections.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_dbNameField, 0 );
    m_getCollectionsBut.setLayoutData( fd );

    m_getCollectionsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        // setupMappingNamesForTable(false);
        setupCollectionNamesForDB( false );
      }
    } );

    m_collectionField = new CCombo( wOutputComp, SWT.BORDER );
    props.setLook( m_collectionField );
    m_collectionField.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();

        m_collectionField.setToolTipText( transMeta.environmentSubstitute( m_collectionField.getText() ) );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_dbNameField, margin );
    fd.right = new FormAttachment( m_getCollectionsBut, -margin );
    m_collectionField.setLayoutData( fd );

    // batch insert line
    Label batchLab = new Label( wOutputComp, SWT.RIGHT );
    batchLab.setText( getString( "MongoDbOutputDialog.BatchInsertSize.Label" ) ); //$NON-NLS-1$
    props.setLook( batchLab );
    batchLab.setToolTipText( getString( "MongoDbOutputDialog.BatchInsertSize.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_collectionField, margin );
    fd.right = new FormAttachment( middle, -margin );
    batchLab.setLayoutData( fd );

    m_batchInsertSizeField = new TextVar( transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_batchInsertSizeField );
    m_batchInsertSizeField.addModifyListener( lsMod );
    // set the tool tip to the contents with any env variables expanded
    m_batchInsertSizeField.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_batchInsertSizeField.setToolTipText( transMeta.environmentSubstitute( m_batchInsertSizeField.getText() ) );
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_collectionField, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_batchInsertSizeField.setLayoutData( fd );

    // truncate line
    Label truncateLab = new Label( wOutputComp, SWT.RIGHT );
    truncateLab.setText( getString( "MongoDbOutputDialog.Truncate.Label" ) ); //$NON-NLS-1$
    props.setLook( truncateLab );
    truncateLab.setToolTipText( getString( "MongoDbOutputDialog.Truncate.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_batchInsertSizeField, margin );
    fd.right = new FormAttachment( middle, -margin );
    truncateLab.setLayoutData( fd );

    m_truncateBut = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_truncateBut );
    m_truncateBut.setToolTipText( getString( "MongoDbOutputDialog.Truncate.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_batchInsertSizeField, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_truncateBut.setLayoutData( fd );
    m_truncateBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );

    // update line
    Label updateLab = new Label( wOutputComp, SWT.RIGHT );
    updateLab.setText( getString( "MongoDbOutputDialog.Update.Label" ) ); //$NON-NLS-1$
    props.setLook( updateLab );
    updateLab.setToolTipText( getString( "MongoDbOutputDialog.Update.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_truncateBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    updateLab.setLayoutData( fd );

    m_updateBut = new Button( wOutputComp, SWT.CHECK );
    m_updateBut = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_updateBut );
    m_updateBut.setToolTipText( getString( "MongoDbOutputDialog.Update.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_truncateBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_updateBut.setLayoutData( fd );

    // multi update can only be used when the update document
    // contains modifier operations:
    // http://docs.mongodb.org/manual/reference/method/db.collection.update/#multi-parameter
    m_updateBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
        m_upsertBut.setEnabled( m_updateBut.getSelection() );
        m_modifierUpdateBut.setEnabled( m_updateBut.getSelection() );
        m_readPreference.setEnabled( m_modifierUpdateBut.getEnabled() && m_modifierUpdateBut.getSelection() );
        m_multiBut.setEnabled( m_updateBut.getSelection() );
        if ( !m_updateBut.getSelection() ) {
          m_modifierUpdateBut.setSelection( false );
          m_multiBut.setSelection( false );
          m_upsertBut.setSelection( false );
        }
        m_multiBut.setEnabled( m_modifierUpdateBut.getSelection() );
        if ( !m_multiBut.getEnabled() ) {
          m_multiBut.setSelection( false );
        }
      }
    } );

    // upsert line
    Label upsertLab = new Label( wOutputComp, SWT.RIGHT );
    upsertLab.setText( getString( "MongoDbOutputDialog.Upsert.Label" ) ); //$NON-NLS-1$
    props.setLook( upsertLab );
    upsertLab.setToolTipText( getString( "MongoDbOutputDialog.Upsert.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_updateBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    upsertLab.setLayoutData( fd );

    m_upsertBut = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_upsertBut );
    m_upsertBut.setToolTipText( getString( "MongoDbOutputDialog.Upsert.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_updateBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_upsertBut.setLayoutData( fd );

    // multi line
    Label multiLab = new Label( wOutputComp, SWT.RIGHT );
    multiLab.setText( getString( "MongoDbOutputDialog.Multi.Label" ) ); //$NON-NLS-1$
    props.setLook( multiLab );
    multiLab.setToolTipText( getString( "MongoDbOutputDialog.Multi.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_upsertBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    multiLab.setLayoutData( fd );

    m_multiBut = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_multiBut );
    m_multiBut.setToolTipText( getString( "MongoDbOutputDialog.Multi.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_upsertBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_multiBut.setLayoutData( fd );
    m_multiBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );

    // modifier update
    Label modifierLab = new Label( wOutputComp, SWT.RIGHT );
    modifierLab.setText( getString( "MongoDbOutputDialog.Modifier.Label" ) ); //$NON-NLS-1$
    props.setLook( modifierLab );
    modifierLab.setToolTipText( getString( "MongoDbOutputDialog.Modifier.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_multiBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    modifierLab.setLayoutData( fd );

    m_modifierUpdateBut = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_modifierUpdateBut );
    m_modifierUpdateBut
      .setToolTipText( getString( "MongoDbOutputDialog.Modifier.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_multiBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_modifierUpdateBut.setLayoutData( fd );
    m_modifierUpdateBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();

        m_multiBut.setEnabled( m_modifierUpdateBut.getSelection() );

        if ( !m_modifierUpdateBut.getSelection() ) {
          m_multiBut.setSelection( false );
        }

        m_readPreference.setEnabled( m_modifierUpdateBut.getEnabled() && m_modifierUpdateBut.getSelection() );
      }
    } );

    // write concern
    Label writeConcernLab = new Label( wOutputComp, SWT.RIGHT );
    writeConcernLab.setText( getString( "MongoDbOutputDialog.WriteConcern.Label" ) ); //$NON-NLS-1$
    writeConcernLab.setToolTipText( getString( "MongoDbOutputDialog.WriteConcern.TipText" ) ); //$NON-NLS-1$
    props.setLook( writeConcernLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_modifierUpdateBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    writeConcernLab.setLayoutData( fd );

    Button getCustomWCBut = new Button( wOutputComp, SWT.PUSH | SWT.CENTER );
    props.setLook( getCustomWCBut );
    getCustomWCBut.setText( getString( "MongoDbOutputDialog.WriteConcern.CustomWriteConcerns" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_modifierUpdateBut, 0 );
    getCustomWCBut.setLayoutData( fd );

    getCustomWCBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        setupCustomWriteConcernNames();
      }
    } );

    m_writeConcern = new CCombo( wOutputComp, SWT.BORDER );
    props.setLook( m_writeConcern );
    m_writeConcern.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( getCustomWCBut, 0 );
    fd.top = new FormAttachment( m_modifierUpdateBut, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_writeConcern.setLayoutData( fd );

    // wTimeout
    Label wTimeoutLab = new Label( wOutputComp, SWT.RIGHT );
    wTimeoutLab.setText( getString( "MongoDbOutputDialog.WTimeout.Label" ) ); //$NON-NLS-1$
    wTimeoutLab.setToolTipText( getString( "MongoDbOutputDialog.WTimeout.TipText" ) ); //$NON-NLS-1$
    props.setLook( wTimeoutLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_writeConcern, margin );
    fd.right = new FormAttachment( middle, -margin );
    wTimeoutLab.setLayoutData( fd );

    m_wTimeout = new TextVar( transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wTimeout );
    m_wTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_writeConcern, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_wTimeout.setLayoutData( fd );
    m_wTimeout.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_wTimeout.setToolTipText( transMeta.environmentSubstitute( m_wTimeout.getText() ) );
      }
    } );

    Label journalWritesLab = new Label( wOutputComp, SWT.RIGHT );
    journalWritesLab.setText( getString( "MongoDbOutputDialog.JournalWrites.Label" ) ); //$NON-NLS-1$
    journalWritesLab
      .setToolTipText( getString( "MongoDbOutputDialog.JournalWrites.TipText" ) ); //$NON-NLS-1$
    props.setLook( journalWritesLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_wTimeout, margin );
    fd.right = new FormAttachment( middle, -margin );
    journalWritesLab.setLayoutData( fd );

    m_journalWritesCheck = new Button( wOutputComp, SWT.CHECK );
    props.setLook( m_journalWritesCheck );
    m_journalWritesCheck.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_wTimeout, margin );
    fd.left = new FormAttachment( middle, 0 );
    m_journalWritesCheck.setLayoutData( fd );

    // read preference
    Label readPrefL = new Label( wOutputComp, SWT.RIGHT );
    readPrefL.setText( getString( "MongoDbOutputDialog.ReadPreferenceLabel" ) ); //$NON-NLS-1$
    readPrefL.setToolTipText(
      getString( "MongoDbOutputDialog.ReadPreferenceLabel.TipText" ) ); //$NON-NLS-1$
    props.setLook( readPrefL );
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( m_journalWritesCheck, margin );
    fd.right = new FormAttachment( middle, -margin );
    readPrefL.setLayoutData( fd );

    m_readPreference = new CCombo( wOutputComp, SWT.BORDER );
    props.setLook( m_readPreference );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_journalWritesCheck, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_readPreference.setLayoutData( fd );
    m_readPreference.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        m_readPreference.setToolTipText( transMeta.environmentSubstitute( m_readPreference.getText() ) );
      }
    } );
    m_readPreference.add( NamedReadPreference.PRIMARY.getName() );
    m_readPreference.add( NamedReadPreference.PRIMARY_PREFERRED.getName() );
    m_readPreference.add( NamedReadPreference.SECONDARY.getName() );
    m_readPreference.add( NamedReadPreference.SECONDARY_PREFERRED.getName() );
    m_readPreference.add( NamedReadPreference.NEAREST.getName() );

    // retries stuff
    Label retriesLab = new Label( wOutputComp, SWT.RIGHT );
    props.setLook( retriesLab );
    retriesLab.setText( getString( "MongoDbOutputDialog.WriteRetries.Label" ) ); //$NON-NLS-1$
    retriesLab
      .setToolTipText( getString( "MongoDbOutputDialog.WriteRetries.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( m_readPreference, margin );
    fd.right = new FormAttachment( middle, -margin );
    retriesLab.setLayoutData( fd );

    m_writeRetries = new TextVar( transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_writeRetries );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_readPreference, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_writeRetries.setLayoutData( fd );
    m_writeRetries.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        m_writeRetries.setToolTipText( transMeta.environmentSubstitute( m_writeRetries.getText() ) );
      }
    } );

    Label retriesDelayLab = new Label( wOutputComp, SWT.RIGHT );
    props.setLook( retriesDelayLab );
    retriesDelayLab.setText( getString( "MongoDbOutputDialog.WriteRetriesDelay.Label" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( m_writeRetries, margin );
    fd.right = new FormAttachment( middle, -margin );
    retriesDelayLab.setLayoutData( fd );

    m_writeRetryDelay = new TextVar( transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_writeRetryDelay );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_writeRetries, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_writeRetryDelay.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wOutputComp.setLayoutData( fd );

    wOutputComp.layout();
    m_wOutputOptionsTab.setControl( wOutputComp );

    // --- start of the fields tab
    m_wMongoFieldsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wMongoFieldsTab.setText( getString( "MongoDbOutputDialog.FieldsTab.TabTitle" ) ); //$NON-NLS-1$
    Composite wFieldsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = 3;
    filterLayout.marginHeight = 3;
    wFieldsComp.setLayout( filterLayout );

    final ColumnInfo[]
      colInf =
      new ColumnInfo[] {
        new ColumnInfo( getString( "MongoDbOutputDialog.Fields.Incoming" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( getString( "MongoDbOutputDialog.Fields.Path" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.UseIncomingName", "Y", "N" ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.NullValues",
          getString( "MongoDbOutputDialog.Fields.NullValues.Insert" ),
          getString( "MongoDbOutputDialog.Fields.NullValues.Ignore" ) ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.JSON", "Y", "N" ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.UpdateMatchField", "Y", "N" ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.ModifierUpdateOperation",
          "N/A", "$set", "$inc", "$push" ),
        createReadOnlyComboBox( "MongoDbOutputDialog.Fields.ModifierApplyPolicy",
          "Insert&Update", "Insert", "Update" )
      };

    // get fields but
    m_getFieldsBut = new Button( wFieldsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getFieldsBut );
    m_getFieldsBut.setText( getString( "MongoDbOutputDialog.GetFieldsBut" ) ); //$NON-NLS-1$
    fd = new FormData();
    // fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.left = new FormAttachment( 0, margin );
    m_getFieldsBut.setLayoutData( fd );

    m_getFieldsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        getFields();
      }
    } );

    m_previewDocStructBut = new Button( wFieldsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_previewDocStructBut );
    m_previewDocStructBut
      .setText( getString( "MongoDbOutputDialog.PreviewDocStructBut" ) ); //$NON-NLS-1$
    fd = new FormData();
    // fd.right = new FormAttachment(100, 0);
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.left = new FormAttachment( m_getFieldsBut, margin );
    m_previewDocStructBut.setLayoutData( fd );
    m_previewDocStructBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        previewDocStruct();
      }
    } );

    m_mongoFieldsView =
      new TableView( transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colInf, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( 0, margin * 2 );
    fd.bottom = new FormAttachment( m_getFieldsBut, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_mongoFieldsView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fd );

    wFieldsComp.layout();
    m_wMongoFieldsTab.setControl( wFieldsComp );

    // indexes tab ------------------
    m_wMongoIndexesTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wMongoIndexesTab
      .setText( getString( "MongoDbOutputDialog.IndexesTab.TabTitle" ) ); //$NON-NLS-1$
    Composite wIndexesComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wIndexesComp );
    FormLayout indexesLayout = new FormLayout();
    indexesLayout.marginWidth = 3;
    indexesLayout.marginHeight = 3;
    wIndexesComp.setLayout( indexesLayout );
    final ColumnInfo[]
      colInf2 =
      new ColumnInfo[] {
        new ColumnInfo( getString( "MongoDbOutputDialog.Indexes.IndexFields" ), //$NON-NLS-1$
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( getString( "MongoDbOutputDialog.Indexes.IndexOpp" ), //$NON-NLS-1$
          ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
        new ColumnInfo( getString( "MongoDbOutputDialog.Indexes.Unique" ), //$NON-NLS-1$
          ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
        new ColumnInfo( getString( "MongoDbOutputDialog.Indexes.Sparse" ), //$NON-NLS-1$
          ColumnInfo.COLUMN_TYPE_CCOMBO, false ), };
    colInf2[ 1 ].setComboValues( new String[] { "Create", "Drop" } ); //$NON-NLS-1$ //$NON-NLS-2$
    colInf2[ 1 ].setReadOnly( true );
    colInf2[ 2 ].setComboValues( new String[] { "Y", "N" } ); //$NON-NLS-1$ //$NON-NLS-2$
    colInf2[ 2 ].setReadOnly( true );
    colInf2[ 3 ].setComboValues( new String[] { "Y", "N" } ); //$NON-NLS-1$ //$NON-NLS-2$
    colInf2[ 3 ].setReadOnly( true );

    // get indexes but
    m_showIndexesBut = new Button( wIndexesComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_showIndexesBut );
    m_showIndexesBut.setText( getString( "MongoDbOutputDialog.ShowIndexesBut" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.left = new FormAttachment( 0, margin );
    m_showIndexesBut.setLayoutData( fd );

    m_showIndexesBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        showIndexInfo();
      }
    } );

    m_mongoIndexesView =
      new TableView( transMeta, wIndexesComp, SWT.FULL_SELECTION | SWT.MULTI, colInf2, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( 0, margin * 2 );
    fd.bottom = new FormAttachment( m_showIndexesBut, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_mongoIndexesView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wIndexesComp.setLayoutData( fd );

    wIndexesComp.layout();
    m_wMongoIndexesTab.setControl( wIndexesComp );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_stepnameText, margin );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    m_wTabFolder.setLayoutData( fd );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( getString( "System.Button.OK" ) ); //$NON-NLS-1$

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( getString( "System.Button.Cancel" ) ); //$NON-NLS-1$

    setButtonPositions( new Button[] { wOK, wCancel }, margin, m_wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      @Override public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOK = new Listener() {
      @Override public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    m_stepnameText.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    m_wTabFolder.setSelection( 0 );
    setSize();

    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }

  private ColumnInfo createReadOnlyComboBox( String i18nKey, String... values ) {
    ColumnInfo info =
      new ColumnInfo( getString( i18nKey ), ColumnInfo.COLUMN_TYPE_CCOMBO, false );
    info.setReadOnly( true );
    info.setComboValues( values );
    return info;
  }

  protected void cancel() {
    stepname = null;
    m_currentMeta.setChanged( changed );

    dispose();
  }

  private void ok() {
    if ( Const.isEmpty( m_stepnameText.getText() ) ) {
      return;
    }

    stepname = m_stepnameText.getText();

    getInfo( m_currentMeta );

    if ( m_currentMeta.getMongoFields() == null ) {
      // popup dialog warning that no paths have been defined
      ShowMessageDialog
        smd =
        new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
          getString( "MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined.Title" ),
          getString( "MongoDbOutputDialog.ErrorMessage.NoFieldPathsDefined" ) ); //$NON-NLS-1$
      smd.open();
    }

    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  private void getInfo( MongoDbOutputMeta meta ) {
    meta.setHostnames( m_hostnameField.getText() );
    meta.setPort( m_portField.getText() );
    meta.setUseAllReplicaSetMembers( m_useAllReplicaSetMembersBut.getSelection() );
    meta.setAuthenticationDatabaseName( m_authDbName.getText() );
    meta.setAuthenticationUser( m_usernameField.getText() );
    meta.setAuthenticationPassword( m_passField.getText() );
    meta.setAuthenticationMechanism( m_dbAuthMec.getText() );
    meta.setUseKerberosAuthentication( m_kerberosBut.getSelection() );
    meta.setDbName( m_dbNameField.getText() );
    meta.setCollection( m_collectionField.getText() );
    meta.setBatchInsertSize( m_batchInsertSizeField.getText() );
    meta.setUpdate( m_updateBut.getSelection() );
    meta.setUpsert( m_upsertBut.getSelection() );
    meta.setMulti( m_multiBut.getSelection() );
    meta.setTruncate( m_truncateBut.getSelection() );
    meta.setModifierUpdate( m_modifierUpdateBut.getSelection() );
    meta.setConnectTimeout( m_connectTimeout.getText() );
    meta.setSocketTimeout( m_socketTimeout.getText() );
    meta.setWriteConcern( m_writeConcern.getText() );
    meta.setWTimeout( m_wTimeout.getText() );
    meta.setJournal( m_journalWritesCheck.getSelection() );
    meta.setReadPreference( m_readPreference.getText() );
    meta.setWriteRetries( m_writeRetries.getText() );
    meta.setWriteRetryDelay( m_writeRetryDelay.getText() );

    meta.setMongoFields( tableToMongoFieldList() );

    // indexes
    int numNonEmpty = m_mongoIndexesView.nrNonEmpty();
    List<MongoDbOutputMeta.MongoIndex> mongoIndexes = new ArrayList<MongoDbOutputMeta.MongoIndex>();
    if ( numNonEmpty > 0 ) {
      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_mongoIndexesView.getNonEmpty( i );

        String indexFieldList = item.getText( 1 ).trim();
        String indexOpp = item.getText( 2 ).trim();
        String unique = item.getText( 3 ).trim();
        String sparse = item.getText( 4 ).trim();

        MongoDbOutputMeta.MongoIndex newIndex = new MongoDbOutputMeta.MongoIndex();
        newIndex.m_pathToFields = indexFieldList;
        newIndex.m_drop = indexOpp.equals( "Drop" ); //$NON-NLS-1$
        newIndex.m_unique = unique.equals( "Y" ); //$NON-NLS-1$
        newIndex.m_sparse = sparse.equals( "Y" ); //$NON-NLS-1$

        mongoIndexes.add( newIndex );
      }
    }
    meta.setMongoIndexes( mongoIndexes );
  }

  private List<MongoDbOutputMeta.MongoField> tableToMongoFieldList() {
    int numNonEmpty = m_mongoFieldsView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      List<MongoDbOutputMeta.MongoField> mongoFields = new ArrayList<MongoDbOutputMeta.MongoField>( numNonEmpty );

      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_mongoFieldsView.getNonEmpty( i );
        String incoming = item.getText( 1 ).trim();
        String path = item.getText( 2 ).trim();
        String useIncoming = item.getText( 3 ).trim();
        String allowNull = item.getText( 4 ).trim();
        String json = item.getText( 5 ).trim();
        String updateMatch = item.getText( 6 ).trim();
        String modifierOp = item.getText( 7 ).trim();
        String modifierPolicy = item.getText( 8 ).trim();

        MongoDbOutputMeta.MongoField newField = new MongoDbOutputMeta.MongoField();
        newField.m_incomingFieldName = incoming;
        newField.m_mongoDocPath = path;
        newField.m_useIncomingFieldNameAsMongoFieldName =
          ( ( useIncoming.length() > 0 ) ? useIncoming.equals( "Y" ) : true ); //$NON-NLS-1$
        newField.insertNull = getString( "MongoDbOutputDialog.Fields.NullValues.Insert" ).equals( allowNull );
        newField.m_JSON = ( ( json.length() > 0 ) ? json.equals( "Y" ) : false ); //$NON-NLS-1$
        newField.m_updateMatchField = ( updateMatch.equals( "Y" ) ); //$NON-NLS-1$
        if ( modifierOp.length() == 0 ) {
          newField.m_modifierUpdateOperation = "N/A"; //$NON-NLS-1$
        } else {
          newField.m_modifierUpdateOperation = modifierOp;
        }
        newField.m_modifierOperationApplyPolicy = modifierPolicy;
        mongoFields.add( newField );
      }

      return mongoFields;
    }

    return null;
  }

  private void getData() {
    m_hostnameField.setText( Const.NVL( m_currentMeta.getHostnames(), "" ) ); //$NON-NLS-1$
    m_portField.setText( Const.NVL( m_currentMeta.getPort(), "" ) ); //$NON-NLS-1$
    m_useAllReplicaSetMembersBut.setSelection( m_currentMeta.getUseAllReplicaSetMembers() );
    m_authDbName.setText( Const.NVL( m_currentMeta.getAuthenticationDatabaseName(), "" ) ); //$NON-NLS-1$
    m_usernameField.setText( Const.NVL( m_currentMeta.getAuthenticationUser(), "" ) ); //$NON-NLS-1$
    m_passField.setText( Const.NVL( m_currentMeta.getAuthenticationPassword(), "" ) ); //$NON-NLS-1$
    m_dbAuthMec.setText( Const.NVL( m_currentMeta.getAuthenticationMechanism(), "" ) );
    m_kerberosBut.setSelection( m_currentMeta.getUseKerberosAuthentication() );
    m_passField.setEnabled( !m_kerberosBut.getSelection() );
    m_dbNameField.setText( Const.NVL( m_currentMeta.getDbName(), "" ) ); //$NON-NLS-1$
    m_collectionField.setText( Const.NVL( m_currentMeta.getCollection(), "" ) ); //$NON-NLS-1$
    m_batchInsertSizeField.setText( Const.NVL( m_currentMeta.getBatchInsertSize(), "" ) ); //$NON-NLS-1$
    m_updateBut.setSelection( m_currentMeta.getUpdate() );
    m_upsertBut.setSelection( m_currentMeta.getUpsert() );
    m_multiBut.setSelection( m_currentMeta.getMulti() );
    m_truncateBut.setSelection( m_currentMeta.getTruncate() );
    m_modifierUpdateBut.setSelection( m_currentMeta.getModifierUpdate() );

    m_upsertBut.setEnabled( m_updateBut.getSelection() );
    m_modifierUpdateBut.setEnabled( m_updateBut.getSelection() );
    m_multiBut.setEnabled( m_updateBut.getSelection() );
    if ( !m_updateBut.getSelection() ) {
      m_modifierUpdateBut.setSelection( false );
      m_multiBut.setSelection( false );
    }
    m_multiBut.setEnabled( m_modifierUpdateBut.getSelection() );
    if ( !m_multiBut.getEnabled() ) {
      m_multiBut.setSelection( false );
    }

    m_readPreference.setEnabled( m_modifierUpdateBut.getEnabled() && m_modifierUpdateBut.getSelection() );
    m_connectTimeout.setText( Const.NVL( m_currentMeta.getConnectTimeout(), "" ) ); //$NON-NLS-1$
    m_socketTimeout.setText( Const.NVL( m_currentMeta.getSocketTimeout(), "" ) ); //$NON-NLS-1$
    m_writeConcern.setText( Const.NVL( m_currentMeta.getWriteConcern(), "" ) ); //$NON-NLS-1$
    m_wTimeout.setText( Const.NVL( m_currentMeta.getWTimeout(), "" ) ); //$NON-NLS-1$
    m_journalWritesCheck.setSelection( m_currentMeta.getJournal() );
    m_readPreference.setText( Const.NVL( m_currentMeta.getReadPreference(), "" ) ); //$NON-NLS-1$
    m_writeRetries.setText( Const.NVL( m_currentMeta.getWriteRetries(), "" //$NON-NLS-1$
      + MongoDbOutputMeta.RETRIES ) );
    m_writeRetryDelay.setText( Const.NVL( m_currentMeta.getWriteRetryDelay(), "" //$NON-NLS-1$
      + MongoDbOutputMeta.RETRIES ) );

    List<MongoDbOutputMeta.MongoField> mongoFields = m_currentMeta.getMongoFields();

    if ( mongoFields != null && mongoFields.size() > 0 ) {
      for ( MongoDbOutputMeta.MongoField field : mongoFields ) {
        TableItem item = new TableItem( m_mongoFieldsView.table, SWT.NONE );

        item.setText( 1, Const.NVL( field.m_incomingFieldName, "" ) ); //$NON-NLS-1$
        item.setText( 2, Const.NVL( field.m_mongoDocPath, "" ) ); //$NON-NLS-1$
        item.setText( 3, field.m_useIncomingFieldNameAsMongoFieldName ? "Y" : "N" ); //$NON-NLS-1$ //$NON-NLS-2$
        String insertNullKey = field.insertNull
          ? "MongoDbOutputDialog.Fields.NullValues.Insert"
          : "MongoDbOutputDialog.Fields.NullValues.Ignore";
        item.setText( 4, getString( insertNullKey ) );
        item.setText( 5, field.m_JSON ? "Y" : "N" ); //$NON-NLS-1$ //$NON-NLS-2$
        item.setText( 6, field.m_updateMatchField ? "Y" : "N" ); //$NON-NLS-1$ //$NON-NLS-2$
        item.setText( 7, Const.NVL( field.m_modifierUpdateOperation, "" ) ); //$NON-NLS-1$
        item.setText( 8, Const.NVL( field.m_modifierOperationApplyPolicy, "" ) ); //$NON-NLS-1$
      }

      m_mongoFieldsView.removeEmptyRows();
      m_mongoFieldsView.setRowNums();
      m_mongoFieldsView.optWidth( true );
    }

    List<MongoDbOutputMeta.MongoIndex> mongoIndexes = m_currentMeta.getMongoIndexes();

    if ( mongoIndexes != null && mongoIndexes.size() > 0 ) {
      for ( MongoDbOutputMeta.MongoIndex index : mongoIndexes ) {
        TableItem item = new TableItem( m_mongoIndexesView.table, SWT.None );

        item.setText( 1, Const.NVL( index.m_pathToFields, "" ) ); //$NON-NLS-1$
        if ( index.m_drop ) {
          item.setText( 2, "Drop" ); //$NON-NLS-1$
        } else {
          item.setText( 2, "Create" ); //$NON-NLS-1$
        }

        item.setText( 3, Const.NVL( index.m_unique ? "Y" : "N", "N" ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        item.setText( 4, Const.NVL( index.m_sparse ? "Y" : "N", "N" ) ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      }

      m_mongoIndexesView.removeEmptyRows();
      m_mongoIndexesView.setRowNums();
      m_mongoIndexesView.optWidth( true );
    }
  }

  private void setupCollectionNamesForDB( boolean quiet ) {
    final String hostname = transMeta.environmentSubstitute( m_hostnameField.getText() );
    final String dB = transMeta.environmentSubstitute( m_dbNameField.getText() );

    String current = m_collectionField.getText();
    m_collectionField.removeAll();

    if ( !Const.isEmpty( hostname ) && !Const.isEmpty( dB ) ) {

      final MongoDbOutputMeta meta = new MongoDbOutputMeta();
      getInfo( meta );
      try {
        MongoClientWrapper clientWrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        Set<String> collections = new HashSet<String>();
        try {
          collections = clientWrapper.getCollectionsNames( dB );
        } finally {
          clientWrapper.dispose();
        }

        for ( String c : collections ) {
          m_collectionField.add( c );
        }
      } catch ( Exception e ) {
        // Unwrap the PrivilegedActionException if it was thrown
        if ( e instanceof PrivilegedActionException ) {
          e = ( (PrivilegedActionException) e ).getException();
        }
        logError( getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ),
          //$NON-NLS-1$ //$NON-NLS-2$
          getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      // popup some feedback

      String missingConnDetails = ""; //$NON-NLS-1$
      if ( Const.isEmpty( hostname ) ) {
        missingConnDetails += "host name(s)"; //$NON-NLS-1$
      }
      if ( Const.isEmpty( dB ) ) {
        missingConnDetails += " database"; //$NON-NLS-1$
      }
      ShowMessageDialog
        smd =
        new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
          getString( "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
          BaseMessages.getString( PKG, //$NON-NLS-1$
            "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails", missingConnDetails ) ); //$NON-NLS-1$
      smd.open();
    }

    if ( !Const.isEmpty( current ) ) {
      m_collectionField.setText( current );
    }
  }

  private void setupCustomWriteConcernNames() {
    String hostname = transMeta.environmentSubstitute( m_hostnameField.getText() );

    if ( !Const.isEmpty( hostname ) ) {
      MongoDbOutputMeta meta = new MongoDbOutputMeta();
      getInfo( meta );
      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        List<String> custom = new ArrayList<String>();
        try {
          custom = wrapper.getLastErrorModes();
        } finally {
          wrapper.dispose();
        }

        if ( custom.size() > 0 ) {
          String current = m_writeConcern.getText();
          m_writeConcern.removeAll();

          for ( String s : custom ) {
            m_writeConcern.add( s );
          }

          if ( !Const.isEmpty( current ) ) {
            m_writeConcern.setText( current );
          }
        }
      } catch ( Exception e ) {
        logError( getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ),
          //$NON-NLS-1$ //$NON-NLS-2$
          getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      ShowMessageDialog
        smd =
        new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
          getString( "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
          BaseMessages.getString( PKG, //$NON-NLS-1$
            "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails",
            "host name(s)" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      smd.open();
    }
  }

  private void setupDBNames() {
    String current = m_dbNameField.getText();
    m_dbNameField.removeAll();

    String hostname = transMeta.environmentSubstitute( m_hostnameField.getText() );

    if ( !Const.isEmpty( hostname ) ) {
      try {
        final MongoDbOutputMeta meta = new MongoDbOutputMeta();
        getInfo( meta );
        List<String> dbNames = new ArrayList<String>();
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }
        for ( String s : dbNames ) {
          m_dbNameField.add( s );
        }

      } catch ( Exception e ) {
        logError( getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ),
          //$NON-NLS-1$ //$NON-NLS-2$
          getString( "MongoDbOutputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      // popup some feedback
      ShowMessageDialog
        smd =
        new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
          getString( "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
          BaseMessages.getString( PKG, //$NON-NLS-1$
            "MongoDbOutputDialog.ErrorMessage.MissingConnectionDetails",
            "host name(s)" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      smd.open();
    }

    if ( !Const.isEmpty( current ) ) {
      m_dbNameField.setText( current );
    }
  }

  private void getFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        BaseStepDialog.getFieldsFromPrevious( r, m_mongoFieldsView, 1, new int[] { 1 }, null, -1, -1, null );
      }
    } catch ( KettleException e ) {
      logError( getString( "System.Dialog.GetFieldsFailed.Message" ), //$NON-NLS-1$
        e );
      new ErrorDialog( shell, getString( "System.Dialog.GetFieldsFailed.Title" ),
        getString( "System.Dialog.GetFieldsFailed.Message" ), e ); //$NON-NLS-1$
    }
  }

  private static enum Element {
    OPEN_BRACE, CLOSE_BRACE, OPEN_BRACKET, CLOSE_BRACKET, COMMA
  }

  private static void pad( StringBuffer toPad, int numBlanks ) {
    for ( int i = 0; i < numBlanks; i++ ) {
      toPad.append( ' ' );
    }
  }

  /**
   * Format JSON document structure for printing to the preview dialog
   *
   * @param toFormat the document to format
   * @return a String containing the formatted document structure
   */
  public static String prettyPrintDocStructure( String toFormat ) {
    StringBuffer result = new StringBuffer();
    int indent = 0;
    String source = toFormat.replaceAll( "[ ]*,", "," ); //$NON-NLS-1$ //$NON-NLS-2$
    Element next = Element.OPEN_BRACE;

    while ( source.length() > 0 ) {
      source = source.trim();
      String toIndent = ""; //$NON-NLS-1$
      int minIndex = Integer.MAX_VALUE;
      char targetChar = '{';
      if ( source.indexOf( '{' ) > -1 && source.indexOf( '{' ) < minIndex ) {
        next = Element.OPEN_BRACE;
        minIndex = source.indexOf( '{' );
        targetChar = '{';
      }
      if ( source.indexOf( '}' ) > -1 && source.indexOf( '}' ) < minIndex ) {
        next = Element.CLOSE_BRACE;
        minIndex = source.indexOf( '}' );
        targetChar = '}';
      }
      if ( source.indexOf( '[' ) > -1 && source.indexOf( '[' ) < minIndex ) {
        next = Element.OPEN_BRACKET;
        minIndex = source.indexOf( '[' );
        targetChar = '[';
      }
      if ( source.indexOf( ']' ) > -1 && source.indexOf( ']' ) < minIndex ) {
        next = Element.CLOSE_BRACKET;
        minIndex = source.indexOf( ']' );
        targetChar = ']';
      }
      if ( source.indexOf( ',' ) > -1 && source.indexOf( ',' ) < minIndex ) {
        next = Element.COMMA;
        minIndex = source.indexOf( ',' );
        targetChar = ',';
      }

      if ( minIndex == 0 ) {
        if ( next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET ) {
          indent -= 2;
        }
        pad( result, indent );
        String comma = ""; //$NON-NLS-1$
        int offset = 1;
        if ( source.length() >= 2 && source.charAt( 1 ) == ',' ) {
          comma = ","; //$NON-NLS-1$
          offset = 2;
        }
        result.append( targetChar ).append( comma ).append( "\n" ); //$NON-NLS-1$
        source = source.substring( offset, source.length() );
      } else {
        pad( result, indent );
        if ( next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET ) {
          toIndent = source.substring( 0, minIndex );
          source = source.substring( minIndex, source.length() );
        } else {
          toIndent = source.substring( 0, minIndex + 1 );
          source = source.substring( minIndex + 1, source.length() );
        }
        result.append( toIndent.trim() ).append( "\n" ); //$NON-NLS-1$
      }

      if ( next == Element.OPEN_BRACE || next == Element.OPEN_BRACKET ) {
        indent += 2;
      }
    }

    return result.toString();
  }

  private void previewDocStruct() {
    List<MongoDbOutputMeta.MongoField> mongoFields = tableToMongoFieldList();

    if ( mongoFields == null || mongoFields.size() == 0 ) {
      return;
    }

    // Try and get meta data on incoming fields
    RowMetaInterface actualR = null;
    RowMetaInterface r;
    boolean gotGenuineRowMeta = false;
    try {
      actualR = transMeta.getPrevStepFields( stepname );
      gotGenuineRowMeta = true;
    } catch ( KettleException e ) {
      // don't complain if we can't
    }
    r = new RowMeta();

    Object[] dummyRow = new Object[ mongoFields.size() ];
    int i = 0;
    try {
      boolean hasTopLevelJSONDocInsert = MongoDbOutputData.scanForInsertTopLevelJSONDoc( mongoFields );

      for ( MongoDbOutputMeta.MongoField field : mongoFields ) {
        // set up dummy row meta
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta( ValueMetaInterface.TYPE_STRING );
        vm.setName( field.m_incomingFieldName );
        r.addValueMeta( vm );

        String val = ""; //$NON-NLS-1$
        if ( gotGenuineRowMeta && actualR.indexOfValue( field.m_incomingFieldName ) >= 0 ) {
          int index = actualR.indexOfValue( field.m_incomingFieldName );
          switch ( actualR.getValueMeta( index ).getType() ) {
            case ValueMetaInterface.TYPE_STRING:
              if ( field.m_JSON ) {
                if ( !field.m_useIncomingFieldNameAsMongoFieldName && Const.isEmpty( field.m_mongoDocPath ) ) {
                  // we will actually have to parse some kind of JSON doc
                  // here in the case where the matching doc/doc to be inserted is
                  // a full top-level incoming JSON doc
                  val = "{\"IncomingJSONDoc\" : \"<document content>\"}"; //$NON-NLS-1$
                } else {
                  val = "<JSON sub document>"; //$NON-NLS-1$
                  // turn this off for the purpose of doc structure
                  // visualization so that we don't screw up for the
                  // lack of a real JSON doc to parse :-)
                  field.m_JSON = false;
                }
              } else {
                val = "<string val>"; //$NON-NLS-1$
              }
              break;
            case ValueMetaInterface.TYPE_INTEGER:
              val = "<integer val>"; //$NON-NLS-1$
              break;
            case ValueMetaInterface.TYPE_NUMBER:
              val = "<number val>"; //$NON-NLS-1$
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              val = "<bool val>"; //$NON-NLS-1$
              break;
            case ValueMetaInterface.TYPE_DATE:
              val = "<date val>"; //$NON-NLS-1$
              break;
            case ValueMetaInterface.TYPE_BINARY:
              val = "<binary val>"; //$NON-NLS-1$
              break;
            default:
              val = "<unsupported value type>"; //$NON-NLS-1$
          }
        } else {
          val = "<value>"; //$NON-NLS-1$
        }

        dummyRow[ i++ ] = val;
      }

      VariableSpace vs = new Variables();
      MongoDbOutputData.MongoTopLevel topLevelStruct = MongoDbOutputData.checkTopLevelConsistency( mongoFields, vs );
      for ( MongoDbOutputMeta.MongoField m : mongoFields ) {
        m.m_modifierOperationApplyPolicy = "Insert&Update"; //$NON-NLS-1$
        m.init( vs );
      }

      String toDisplay = ""; //$NON-NLS-1$
      String windowTitle = getString( "MongoDbOutputDialog.PreviewDocStructure.Title" ); //$NON-NLS-1$
      // if (!m_currentMeta.getModifierUpdate()) {
      if ( !m_modifierUpdateBut.getSelection() ) {
        DBObject
          result =
          MongoDbOutputData
            .kettleRowToMongo( mongoFields, r, dummyRow, vs, topLevelStruct, hasTopLevelJSONDocInsert );
        toDisplay = prettyPrintDocStructure( result.toString() );
      } else {
        DBObject query = MongoDbOutputData.getQueryObject( mongoFields, r, dummyRow, vs, topLevelStruct );
        DBObject
          modifier =
          new MongoDbOutputData().getModifierUpdateObject( mongoFields, r, dummyRow, vs, topLevelStruct );
        toDisplay = getString( "MongoDbOutputDialog.PreviewModifierUpdate.Heading1" ) //$NON-NLS-1$
          + ":\n\n" //$NON-NLS-1$
          + prettyPrintDocStructure( query.toString() )
          + getString( "MongoDbOutputDialog.PreviewModifierUpdate.Heading2" ) //$NON-NLS-1$
          + ":\n\n" //$NON-NLS-1$
          + prettyPrintDocStructure( modifier.toString() );
        windowTitle = getString( "MongoDbOutputDialog.PreviewModifierUpdate.Title" ); //$NON-NLS-1$
      }

      ShowMessageDialog
        smd =
        new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK, windowTitle, toDisplay, true );
      smd.open();
    } catch ( Exception ex ) {
      logError( getString( "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message" )
        //$NON-NLS-1$
        + ":\n\n" + ex.getMessage(), ex ); //$NON-NLS-1$
      new ErrorDialog( shell,
        getString( "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Title" ),
        //$NON-NLS-1$
        getString( "MongoDbOutputDialog.ErrorMessage.ProblemPreviewingDocStructure.Message" )
          //$NON-NLS-1$
          + ":\n\n" + ex.getMessage(), ex ); //$NON-NLS-1$
      return;
    }
  }

  private void showIndexInfo() {
    String hostname = transMeta.environmentSubstitute( m_hostnameField.getText() );
    String dbName = transMeta.environmentSubstitute( m_dbNameField.getText() );
    String collection = transMeta.environmentSubstitute( m_collectionField.getText() );

    if ( !Const.isEmpty( hostname ) ) {
      MongoClient conn = null;
      try {
        MongoDbOutputMeta meta = new MongoDbOutputMeta();
        getInfo( meta );
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        StringBuffer result = new StringBuffer();
        for ( String index : wrapper.getIndexInfo( dbName, collection ) ) {
          result.append( index ).append( "\n\n" ); //$NON-NLS-1$
        }

        ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK,
            BaseMessages.getString( PKG, "MongoDbOutputDialog.IndexInfo", collection ), result.toString(),
            true ); //$NON-NLS-1$
        smd.open();
      } catch ( Exception e ) {
        logError( getString( "MongoDbOutputDialog.ErrorMessage.GeneralError.Message" ) //$NON-NLS-1$
          + ":\n\n" + e.getMessage(), e ); //$NON-NLS-1$
        new ErrorDialog( shell, getString( "MongoDbOutputDialog.ErrorMessage.IndexPreview.Title" ),
          //$NON-NLS-1$
          getString( "MongoDbOutputDialog.ErrorMessage.GeneralError.Message" ) //$NON-NLS-1$
            + ":\n\n" + e.getMessage(), e ); //$NON-NLS-1$
      } finally {
        if ( conn != null ) {
          conn.close();
          conn = null;
        }
      }
    }
  }

  private String getString( String key ) {
    return BaseMessages.getString( PKG, key );
  }
}
