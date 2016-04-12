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

package org.pentaho.di.ui.trans.steps.mongodbinput;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
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
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.mongodbinput.DiscoverFieldsCallback;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.StyledTextComp;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProperties;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoWrapperUtil;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MongoDbInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
  // needed by
  // Translator2!!
  // $NON-NLS-1$

  private CTabFolder m_wTabFolder;
  private CTabItem m_wConfigTab;
  private CTabItem m_wInputOptionsTab;
  private CTabItem m_wMongoQueryTab;
  private CTabItem m_wMongoFieldsTab;

  private TextVar wHostname;
  private TextVar wPort;
  private Button m_useAllReplicaSetMembersBut;
  private CCombo wDbName;
  private Button m_getDbsBut;
  private TextVar wFieldsName;
  private CCombo wCollection;
  private Button m_getCollectionsBut;
  private TextVar wJsonField;

  private StyledTextComp wJsonQuery;
  private Label wlJsonQuery;
  private Button m_queryIsPipelineBut;

  private TextVar wAuthDbName;
  private TextVar wAuthUser;
  private TextVar wAuthPass;
  private CCombo  m_dbAuthMec;

  private Button m_kerberosBut;

  private Button m_outputAsJson;
  private TableView m_fieldsView;

  private TextVar m_connectionTimeout;
  private TextVar m_socketTimeout;
  private CCombo m_readPreference;

  private TableView m_tagsView;
  private ColumnInfo[] m_colInf;

  private Button m_executeForEachRowBut;

  private final MongoDbInputMeta input;
  /* Only referenced in commented code, commenting also
  private CCombo m_tagsCombo;
  private String m_currentTagsState = ""; //$NON-NLS-1$
  */

  public MongoDbInputDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (MongoDbInputMeta) in;
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Shell.Title" ) ); //$NON-NLS-1$

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Stepname.Label" ) ); //$NON-NLS-1$
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;

    m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );
    m_wTabFolder.setSimple( false );

    // start of the config tab
    m_wConfigTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wConfigTab.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.ConfigTab.TabTitle" ) ); //$NON-NLS-1$

    Composite wConfigComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wConfigComp );

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout( configLayout );

    // Hostname(s) input ...
    //
    Label wlHostname = new Label( wConfigComp, SWT.RIGHT );
    wlHostname.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Hostname.Label" ) ); //$NON-NLS-1$
    wlHostname
        .setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.Hostname.Label.TipText" ) ); //$NON-NLS-1$
    props.setLook( wlHostname );
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment( 0, 0 );
    fdlHostname.right = new FormAttachment( middle, -margin );
    fdlHostname.top = new FormAttachment( 0, margin );
    wlHostname.setLayoutData( fdlHostname );
    wHostname = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    wHostname.addModifyListener( lsMod );
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment( middle, 0 );
    fdHostname.top = new FormAttachment( 0, margin );
    fdHostname.right = new FormAttachment( 100, 0 );
    wHostname.setLayoutData( fdHostname );
    lastControl = wHostname;

    // Port input ...
    //
    Label wlPort = new Label( wConfigComp, SWT.RIGHT );
    wlPort.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Port.Label" ) ); //$NON-NLS-1$
    wlPort.setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.Port.Label.TipText" ) ); //$NON-NLS-1$
    props.setLook( wlPort );
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, 0 );
    fdlPort.right = new FormAttachment( middle, -margin );
    fdlPort.top = new FormAttachment( lastControl, margin );
    wlPort.setLayoutData( fdlPort );
    wPort = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( lastControl, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData( fdPort );
    lastControl = wPort;

    // Use all replica set members/mongos check box
    Label useAllReplicaLab = new Label( wConfigComp, SWT.RIGHT );
    useAllReplicaLab
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.UseAllReplicaSetMembers.Label" ) ); //$NON-NLS-1$
    useAllReplicaLab
        .setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.UseAllReplicaSetMembers.TipText" ) );
    props.setLook( useAllReplicaLab );
    FormData fdlRep = new FormData();
    fdlRep.left = new FormAttachment( 0, 0 );
    fdlRep.right = new FormAttachment( middle, -margin );
    fdlRep.top = new FormAttachment( lastControl, margin );
    useAllReplicaLab.setLayoutData( fdlRep );

    m_useAllReplicaSetMembersBut = new Button( wConfigComp, SWT.CHECK );
    props.setLook( m_useAllReplicaSetMembersBut );
    FormData  fdbRep = new FormData();
    fdbRep.left = new FormAttachment( middle, 0 );
    fdbRep.top = new FormAttachment( lastControl, margin );
    fdbRep.right = new FormAttachment( 100, 0 );
    m_useAllReplicaSetMembersBut.setLayoutData( fdbRep );
    lastControl = m_useAllReplicaSetMembersBut;
    m_useAllReplicaSetMembersBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Authentication...
    //

    // AuthDbName line
    Label wlAuthDbName = new Label( wConfigComp, SWT.RIGHT );
    wlAuthDbName.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.AuthenticationDatabaseName.Label" ) ); //$NON-NLS-1$
    props.setLook( wlAuthDbName );
    FormData fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment( 0, -margin );
    fdlAuthUser.top = new FormAttachment( lastControl, margin );
    fdlAuthUser.right = new FormAttachment( middle, -margin );
    wlAuthDbName.setLayoutData( fdlAuthUser );

    wAuthDbName = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAuthDbName );
    wAuthDbName.addModifyListener( lsMod );
    FormData fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment( middle, 0 );
    fdAuthUser.top = new FormAttachment( lastControl, margin );
    fdAuthUser.right = new FormAttachment( 100, 0 );
    wAuthDbName.setLayoutData( fdAuthUser );
    lastControl = wAuthDbName;

    // Authentication Mechanisms
    Label wlAuthMec = new Label( wConfigComp, SWT.RIGHT );
    wlAuthMec.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.AuthMechanism.Label" ) ); //$NON-NLS-1$
    props.setLook( wlAuthMec );
    FormData fd = new FormData();
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

    // AuthUser line
    Label wlAuthUser = new Label( wConfigComp, SWT.RIGHT );
    wlAuthUser.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.AuthenticationUser.Label" ) ); //$NON-NLS-1$
    props.setLook( wlAuthUser );
    fdlAuthUser = new FormData();
    fdlAuthUser.left = new FormAttachment( 0, 0 );
    fdlAuthUser.right = new FormAttachment( middle, -margin );
    fdlAuthUser.top = new FormAttachment( lastControl, margin );
    wlAuthUser.setLayoutData( fdlAuthUser );

    wAuthUser = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAuthUser );
    wAuthUser.addModifyListener( lsMod );
    fdAuthUser = new FormData();
    fdAuthUser.left = new FormAttachment( middle, 0 );
    fdAuthUser.top = new FormAttachment( lastControl, margin );
    fdAuthUser.right = new FormAttachment( 100, 0 );
    wAuthUser.setLayoutData( fdAuthUser );
    lastControl = wAuthUser;

    // AuthPass line
    Label wlAuthPass = new Label( wConfigComp, SWT.RIGHT );
    wlAuthPass
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.AuthenticationPassword.Label" ) ); //$NON-NLS-1$
    props.setLook( wlAuthPass );
    FormData fdlAuthPass = new FormData();
    fdlAuthPass.left = new FormAttachment( 0, -margin );
    fdlAuthPass.top = new FormAttachment( lastControl, margin );
    fdlAuthPass.right = new FormAttachment( middle, -margin );
    wlAuthPass.setLayoutData( fdlAuthPass );

    wAuthPass = new PasswordTextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wAuthPass );
    wAuthPass.addModifyListener( lsMod );
    FormData fdAuthPass = new FormData();
    fdAuthPass.left = new FormAttachment( middle, 0 );
    fdAuthPass.top = new FormAttachment( lastControl, margin );
    fdAuthPass.right = new FormAttachment( 100, 0 );
    wAuthPass.setLayoutData( fdAuthPass );
    lastControl = wAuthPass;

    // use kerberos authentication
    Label kerbLab = new Label( wConfigComp, SWT.RIGHT );
    kerbLab.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Kerberos.Label" ) );
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
        wAuthPass.setEnabled( !m_kerberosBut.getSelection() );
      }
    } );
    lastControl = m_kerberosBut;

    // connection timeout
    Label connectTimeoutL = new Label( wConfigComp, SWT.RIGHT );
    connectTimeoutL
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.ConnectionTimeout.Label" ) ); //$NON-NLS-1$
    props.setLook( connectTimeoutL );
    connectTimeoutL
        .setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.ConnectionTimeout.TipText" ) ); //$NON-NLS-1$

    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    connectTimeoutL.setLayoutData( fd );

    m_connectionTimeout = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_connectionTimeout );
    m_connectionTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_connectionTimeout.setLayoutData( fd );
    lastControl = m_connectionTimeout;

    // socket timeout
    Label socketTimeoutL = new Label( wConfigComp, SWT.RIGHT );
    socketTimeoutL.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.SocketTimeout.Label" ) ); //$NON-NLS-1$
    props.setLook( connectTimeoutL );
    socketTimeoutL
        .setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.SocketTimeout.TipText" ) ); //$NON-NLS-1$

    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    socketTimeoutL.setLayoutData( fd );

    m_socketTimeout = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_socketTimeout );
    m_socketTimeout.addModifyListener( lsMod );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_socketTimeout.setLayoutData( fd );
    lastControl = m_socketTimeout;

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wConfigComp.setLayoutData( fd );

    wConfigComp.layout();
    m_wConfigTab.setControl( wConfigComp );

    // Input options tab -----
    m_wInputOptionsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wInputOptionsTab
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.InputOptionsTab.TabTitle" ) ); //$NON-NLS-1$
    Composite wInputOptionsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wInputOptionsComp );
    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 3;
    inputLayout.marginHeight = 3;
    wInputOptionsComp.setLayout( inputLayout );

    // DbName input ...
    //
    Label wlDbName = new Label( wInputOptionsComp, SWT.RIGHT );
    wlDbName.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.DbName.Label" ) ); //$NON-NLS-1$
    props.setLook( wlDbName );
    FormData fdlDbName = new FormData();
    fdlDbName.left = new FormAttachment( 0, 0 );
    fdlDbName.right = new FormAttachment( middle, -margin );
    fdlDbName.top = new FormAttachment( 0, margin );
    wlDbName.setLayoutData( fdlDbName );

    m_getDbsBut = new Button( wInputOptionsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getDbsBut );
    m_getDbsBut.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.DbName.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    m_getDbsBut.setLayoutData( fd );

    m_getDbsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        setupDBNames();
      }
    } );

    wDbName = new CCombo( wInputOptionsComp, SWT.BORDER );
    props.setLook( wDbName );
    wDbName.addModifyListener( lsMod );
    FormData fdDbName = new FormData();
    fdDbName.left = new FormAttachment( middle, 0 );
    fdDbName.top = new FormAttachment( 0, margin );
    fdDbName.right = new FormAttachment( m_getDbsBut, 0 );
    wDbName.setLayoutData( fdDbName );
    lastControl = wDbName;

    wDbName.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        input.setChanged();
        wDbName.setToolTipText( transMeta.environmentSubstitute( wDbName.getText() ) );
      }
    } );

    // Collection input ...
    //
    Label wlCollection = new Label( wInputOptionsComp, SWT.RIGHT );
    wlCollection.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Collection.Label" ) ); //$NON-NLS-1$
    props.setLook( wlCollection );
    FormData fdlCollection = new FormData();
    fdlCollection.left = new FormAttachment( 0, 0 );
    fdlCollection.right = new FormAttachment( middle, -margin );
    fdlCollection.top = new FormAttachment( lastControl, margin );
    wlCollection.setLayoutData( fdlCollection );

    m_getCollectionsBut = new Button( wInputOptionsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_getCollectionsBut );
    m_getCollectionsBut
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.GetCollections.Button" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( lastControl, 0 );
    m_getCollectionsBut.setLayoutData( fd );

    m_getCollectionsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        setupCollectionNamesForDB();
      }
    } );

    wCollection = new CCombo( wInputOptionsComp, SWT.BORDER );
    props.setLook( wCollection );
    wCollection.addModifyListener( lsMod );
    FormData fdCollection = new FormData();
    fdCollection.left = new FormAttachment( middle, 0 );
    fdCollection.top = new FormAttachment( lastControl, margin );
    fdCollection.right = new FormAttachment( m_getCollectionsBut, 0 );
    wCollection.setLayoutData( fdCollection );
    lastControl = wCollection;

    wCollection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        updateQueryTitleInfo();
      }
    } );

    wCollection.addFocusListener( new FocusListener() {
      @Override public void focusGained( FocusEvent e ) {

      }

      @Override public void focusLost( FocusEvent e ) {
        updateQueryTitleInfo();
      }
    } );

    // read preference
    Label readPrefL = new Label( wInputOptionsComp, SWT.RIGHT );
    readPrefL.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.ReadPreferenceLabel" ) ); //$NON-NLS-1$
    props.setLook( readPrefL );
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    readPrefL.setLayoutData( fd );

    m_readPreference = new CCombo( wInputOptionsComp, SWT.BORDER );
    props.setLook( m_readPreference );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_readPreference.setLayoutData( fd );
    m_readPreference.addModifyListener( new ModifyListener() {
      @Override public void modifyText( ModifyEvent e ) {
        input.setChanged();
        m_readPreference.setToolTipText( transMeta.environmentSubstitute( m_readPreference.getText() ) );
      }
    } );

    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      m_readPreference.add( preference.getName() );
    }

    lastControl = m_readPreference;

    /*
     * // add to table button Button addToTableBut = new
     * Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
     * props.setLook(addToTableBut);
     * addToTableBut.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.AddToTable.Button")); //$NON-NLS-1$ fd = new
     * FormData(); fd.top = new FormAttachment(lastControl, 0); fd.right = new
     * FormAttachment(100, 0); addToTableBut.setLayoutData(fd);
     * 
     * addToTableBut.addSelectionListener(new SelectionAdapter() {
     * 
     * @Override public void widgetSelected(SelectionEvent e) {
     * addTagsToTable(); input.setChanged(); } });
     * 
     * 
     * 
     * // get tags button Button getTagsBut = new Button(wInputOptionsComp,
     * SWT.PUSH | SWT.CENTER); props.setLook(getTagsBut);
     * getTagsBut.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.GetTags.Button")); //$NON-NLS-1$
     * getTagsBut.setToolTipText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.GetTags.Button.TipText")); //$NON-NLS-1$ fd = new
     * FormData(); fd.top = new FormAttachment(lastControl, 0); fd.right = new
     * FormAttachment(addToTableBut, margin); getTagsBut.setLayoutData(fd);
     * 
     * getTagsBut.addSelectionListener(new SelectionAdapter() {
     * 
     * @Override public void widgetSelected(SelectionEvent e) {
     * setupTagSetComboValues(); } });
     * 
     * // tags editor text field/combo Label tagsLabel = new
     * Label(wInputOptionsComp, SWT.RIGHT);
     * tagsLabel.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.TagSetCombo.Label")); //$NON-NLS-1$
     * props.setLook(tagsLabel); fd = new FormData(); fd.left = new
     * FormAttachment(0, -margin); fd.top = new FormAttachment(lastControl,
     * margin); fd.right = new FormAttachment(middle, -margin);
     * tagsLabel.setLayoutData(fd);
     * 
     * m_tagsCombo = new CCombo(wInputOptionsComp, SWT.BORDER);
     * props.setLook(m_tagsCombo); fd = new FormData(); fd.left = new
     * FormAttachment(middle, 0); fd.top = new FormAttachment(lastControl,
     * margin); fd.right = new FormAttachment(getTagsBut, 0);
     * m_tagsCombo.setLayoutData(fd);
     * 
     * lastControl = m_tagsCombo;
     * 
     * m_tagsCombo.addSelectionListener(new SelectionAdapter() {
     * 
     * @Override public void widgetSelected(SelectionEvent e) { String current =
     * m_tagsCombo.getText(); if (!Const.isEmpty(current)) { m_currentTagsState
     * = m_currentTagsState + ((m_currentTagsState.length() > 0) ?
     * ((!m_currentTagsState .endsWith(",")) ? ", " : "") : "") + current;
     * //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
     * m_tagsCombo.setText(m_currentTagsState); } }
     * 
     * @Override public void widgetDefaultSelected(SelectionEvent e) { // listen
     * for enter being pressed in the text field and update // the current tags
     * state variable String current = m_tagsCombo.getText(); if
     * (!Const.isEmpty(current)) { m_currentTagsState = current; } else {
     * m_currentTagsState = ""; //$NON-NLS-1$ } } });
     * 
     * m_tagsCombo.addFocusListener(new FocusListener() { public void
     * focusGained(FocusEvent e) {
     * 
     * }
     * 
     * public void focusLost(FocusEvent e) { m_currentTagsState =
     * m_tagsCombo.getText(); } });
     * 
     * // edit selected button Button editSelectedBut = new
     * Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
     * props.setLook(editSelectedBut);
     * editSelectedBut.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.EditSelected.Button")); //$NON-NLS-1$
     * 
     * fd = new FormData(); fd.bottom = new FormAttachment(100, -margin * 2);
     * fd.left = new FormAttachment(0, 0); editSelectedBut.setLayoutData(fd);
     * 
     * editSelectedBut.addSelectionListener(new SelectionAdapter() {
     * 
     * @Override public void widgetSelected(SelectionEvent e) {
     * moveSelectedTagSetToEditor(); } });
     * 
     * // delete selected button Button deleteSelectedBut = new
     * Button(wInputOptionsComp, SWT.PUSH | SWT.CENTER);
     * props.setLook(deleteSelectedBut);
     * deleteSelectedBut.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.DeleteSelected.Button")); //$NON-NLS-1$
     * 
     * fd = new FormData(); fd.bottom = new FormAttachment(100, -margin * 2);
     * fd.left = new FormAttachment(editSelectedBut, margin);
     * deleteSelectedBut.setLayoutData(fd);
     * 
     * deleteSelectedBut.addSelectionListener(new SelectionAdapter() {
     * 
     * @Override public void widgetSelected(SelectionEvent e) {
     * deleteSelectedFromView(); input.setChanged(); } });
     */
    // test tag set but
    Button testUserTagsBut = new Button( wInputOptionsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( testUserTagsBut );
    testUserTagsBut.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.TestUserTags.Button" ) ); //$NON-NLS-1$
    testUserTagsBut.setToolTipText(
        BaseMessages.getString( PKG, "MongoDbInputDialog.TestUserTags.Button.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.right = new FormAttachment( 100, 0 );
    testUserTagsBut.setLayoutData( fd );

    testUserTagsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        testUserSpecifiedTagSetsAgainstReplicaSet();
      }
    } );

    Button joinTagsBut = new Button( wInputOptionsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( joinTagsBut );
    joinTagsBut.setText( "Join tags" );
    joinTagsBut.setToolTipText( "Join tags" );

    /*
     * joinTagsBut.setText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.GetTags.Button")); //$NON-NLS-1$
     * joinTagsBut.setToolTipText(BaseMessages.getString(PKG,
     * "MongoDbInputDialog.GetTags.Button.TipText")); //$NON-NLS-1$
     */
    fd = new FormData();
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.right = new FormAttachment( testUserTagsBut, margin );
    joinTagsBut.setLayoutData( fd );

    joinTagsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        concatenateTags();
      }
    } );

    Button getTagsBut = new Button( wInputOptionsComp, SWT.PUSH | SWT.CENTER );
    props.setLook( getTagsBut );
    getTagsBut.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.GetTags.Button" ) ); //$NON-NLS-1$
    getTagsBut
        .setToolTipText( BaseMessages.getString( PKG, "MongoDbInputDialog.GetTags.Button.TipText" ) ); //$NON-NLS-1$
    fd = new FormData();
    // fd.top = new FormAttachment(lastControl, 0);
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.right = new FormAttachment( joinTagsBut, margin );
    getTagsBut.setLayoutData( fd );

    getTagsBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        setupTagSetComboValues();
      }
    } );

    m_colInf =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.TagSets.TagSetColumnTitle" ), //$NON-NLS-1$
                ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    m_colInf[0].setReadOnly( false );

    Label tagSetsTitle = new Label( wInputOptionsComp, SWT.LEFT );
    tagSetsTitle.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.TagSets.Title" ) ); //$NON-NLS-1$
    props.setLook( tagSetsTitle );
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.top = new FormAttachment( lastControl, margin );
    fd.right = new FormAttachment( middle, -margin );
    tagSetsTitle.setLayoutData( fd );
    lastControl = tagSetsTitle;

    m_tagsView =
        new TableView( transMeta, wInputOptionsComp, SWT.FULL_SELECTION | SWT.MULTI, m_colInf, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin * 2 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_tagsView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wInputOptionsComp.setLayoutData( fd );

    wInputOptionsComp.layout();
    m_wInputOptionsTab.setControl( wInputOptionsComp );

    // Query tab -----
    m_wMongoQueryTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wMongoQueryTab.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.QueryTab.TabTitle" ) ); //$NON-NLS-1$
    Composite wQueryComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wQueryComp );
    FormLayout queryLayout = new FormLayout();
    queryLayout.marginWidth = 3;
    queryLayout.marginHeight = 3;
    wQueryComp.setLayout( queryLayout );

    // fields input ...
    //
    Label wlFieldsName = new Label( wQueryComp, SWT.RIGHT );
    wlFieldsName.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.FieldsName.Label" ) ); //$NON-NLS-1$
    props.setLook( wlFieldsName );
    FormData fdlFieldsName = new FormData();
    fdlFieldsName.left = new FormAttachment( 0, 0 );
    fdlFieldsName.right = new FormAttachment( middle, -margin );
    fdlFieldsName.bottom = new FormAttachment( 100, -margin );
    wlFieldsName.setLayoutData( fdlFieldsName );
    wFieldsName = new TextVar( transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFieldsName );
    wFieldsName.addModifyListener( lsMod );
    FormData fdFieldsName = new FormData();
    fdFieldsName.left = new FormAttachment( middle, 0 );
    fdFieldsName.bottom = new FormAttachment( 100, -margin );
    fdFieldsName.right = new FormAttachment( 100, 0 );
    wFieldsName.setLayoutData( fdFieldsName );
    lastControl = wFieldsName;

    Label executeForEachRLab = new Label( wQueryComp, SWT.RIGHT );
    executeForEachRLab
        .setText( BaseMessages.getString( PKG, "MongoDbInputDialog.ExecuteForEachRow.Label" ) ); //$NON-NLS-1$
    props.setLook( executeForEachRLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, -margin );
    fd.bottom = new FormAttachment( lastControl, -margin );
    fd.right = new FormAttachment( middle, -margin );
    executeForEachRLab.setLayoutData( fd );

    m_executeForEachRowBut = new Button( wQueryComp, SWT.CHECK );
    props.setLook( m_executeForEachRowBut );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( lastControl, -margin );
    m_executeForEachRowBut.setLayoutData( fd );
    lastControl = m_executeForEachRowBut;

    Label queryIsPipelineL = new Label( wQueryComp, SWT.RIGHT );
    queryIsPipelineL.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Pipeline.Label" ) ); //$NON-NLS-1$
    props.setLook( queryIsPipelineL );
    fd = new FormData();
    fd.bottom = new FormAttachment( lastControl, -margin );
    fd.left = new FormAttachment( 0, -margin );
    fd.right = new FormAttachment( middle, -margin );
    queryIsPipelineL.setLayoutData( fd );

    m_queryIsPipelineBut = new Button( wQueryComp, SWT.CHECK );
    props.setLook( m_queryIsPipelineBut );
    fd = new FormData();
    fd.bottom = new FormAttachment( lastControl, -margin );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_queryIsPipelineBut.setLayoutData( fd );
    lastControl = m_queryIsPipelineBut;

    m_queryIsPipelineBut.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        updateQueryTitleInfo();
      }
    } );

    // JSON Query input ...
    //
    wlJsonQuery = new Label( wQueryComp, SWT.NONE );
    wlJsonQuery.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.JsonQuery.Label" ) ); //$NON-NLS-1$
    props.setLook( wlJsonQuery );
    FormData fdlJsonQuery = new FormData();
    fdlJsonQuery.left = new FormAttachment( 0, 0 );
    fdlJsonQuery.right = new FormAttachment( 100, -margin );
    fdlJsonQuery.top = new FormAttachment( 0, margin );
    wlJsonQuery.setLayoutData( fdlJsonQuery );

    wJsonQuery =
        new StyledTextComp( transMeta, wQueryComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
            "" ); //$NON-NLS-1$
    props.setLook( wJsonQuery, PropsUI.WIDGET_STYLE_FIXED );
    wJsonQuery.addModifyListener( lsMod );

    /*
     * wJsonQuery = new TextVar(transMeta, wQueryComp, SWT.SINGLE | SWT.LEFT |
     * SWT.BORDER); props.setLook(wJsonQuery);
     * wJsonQuery.addModifyListener(lsMod);
     */
    FormData fdJsonQuery = new FormData();
    fdJsonQuery.left = new FormAttachment( 0, 0 );
    fdJsonQuery.top = new FormAttachment( wlJsonQuery, margin );
    fdJsonQuery.right = new FormAttachment( 100, -2 * margin );
    fdJsonQuery.bottom = new FormAttachment( lastControl, -margin );
    // wJsonQuery.setLayoutData(fdJsonQuery);
    wJsonQuery.setLayoutData( fdJsonQuery );
    // lastControl = wJsonQuery;
    lastControl = wJsonQuery;

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wQueryComp.setLayoutData( fd );

    wQueryComp.layout();
    m_wMongoQueryTab.setControl( wQueryComp );

    // fields tab -----
    m_wMongoFieldsTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wMongoFieldsTab.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.FieldsTab.TabTitle" ) ); //$NON-NLS-1$
    Composite wFieldsComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout( fieldsLayout );

    // Output as Json check box
    Label outputJLab = new Label( wFieldsComp, SWT.RIGHT );
    outputJLab.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.OutputJson.Label" ) ); //$NON-NLS-1$
    props.setLook( outputJLab );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    outputJLab.setLayoutData( fd );
    m_outputAsJson = new Button( wFieldsComp, SWT.CHECK );
    props.setLook( m_outputAsJson );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_outputAsJson.setLayoutData( fd );
    lastControl = m_outputAsJson;
    m_outputAsJson.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        wGet.setEnabled( !m_outputAsJson.getSelection() );
        wJsonField.setEnabled( m_outputAsJson.getSelection() );
      }
    } );

    // JsonField input ...
    //
    Label wlJsonField = new Label( wFieldsComp, SWT.RIGHT );
    wlJsonField.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.JsonField.Label" ) ); //$NON-NLS-1$
    props.setLook( wlJsonField );
    FormData fdlJsonField = new FormData();
    fdlJsonField.left = new FormAttachment( 0, 0 );
    fdlJsonField.right = new FormAttachment( middle, -margin );
    fdlJsonField.top = new FormAttachment( lastControl, margin );
    wlJsonField.setLayoutData( fdlJsonField );
    wJsonField = new TextVar( transMeta, wFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wJsonField );
    wJsonField.addModifyListener( lsMod );
    FormData fdJsonField = new FormData();
    fdJsonField.left = new FormAttachment( middle, 0 );
    fdJsonField.top = new FormAttachment( lastControl, margin );
    fdJsonField.right = new FormAttachment( 100, 0 );
    wJsonField.setLayoutData( fdJsonField );
    lastControl = wJsonField;

    // get fields button
    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.Button.GetFields" ) ); //$NON-NLS-1$
    props.setLook( wGet );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fd );
    wGet.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        // populate table from schema
        MongoDbInputMeta newMeta = (MongoDbInputMeta) input.clone();
        getFields( newMeta );
      }
    } );

    // fields stuff
    final ColumnInfo[]
        colinf =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.FIELD_NAME" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.FIELD_PATH" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.FIELD_TYPE" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.FIELD_INDEXED" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.SAMPLE_ARRAYINFO" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.SAMPLE_PERCENTAGE" ), //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "MongoDbInputDialog.Fields.SAMPLE_DISPARATE_TYPES" ),
              //$NON-NLS-1$
              ColumnInfo.COLUMN_TYPE_TEXT, false ), };

    colinf[2].setComboValues( ValueMeta.getTypes() );
    colinf[4].setReadOnly( true );
    colinf[5].setReadOnly( true );
    colinf[6].setReadOnly( true );

    m_fieldsView = new TableView( transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( lastControl, margin * 2 );
    fd.bottom = new FormAttachment( wGet, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_fieldsView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fd );

    wFieldsComp.layout();
    m_wMongoFieldsTab.setControl( wFieldsComp );

    // --------------

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wStepname, margin );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    m_wTabFolder.setLayoutData( fd );

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) ); //$NON-NLS-1$
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$

    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, m_wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      @Override public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsPreview = new Listener() {
      @Override public void handleEvent( Event e ) {
        preview();
      }
    };
    lsOK = new Listener() {
      @Override public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wPreview.addListener( SWT.Selection, lsPreview );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wHostname.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( input );
    input.setChanged( changed );

    m_wTabFolder.setSelection( 0 );
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData( MongoDbInputMeta meta ) {
    wHostname.setText( Const.NVL( meta.getHostnames(), "" ) ); //$NON-NLS-1$
    wPort.setText( Const.NVL( meta.getPort(), "" ) ); //$NON-NLS-1$
    m_useAllReplicaSetMembersBut.setSelection( meta.getUseAllReplicaSetMembers() );
    m_dbAuthMec.setText( Const.NVL( meta.getAuthenticationMechanism(), "" ) );
    wDbName.setText( Const.NVL( meta.getDbName(), "" ) ); //$NON-NLS-1$
    wFieldsName.setText( Const.NVL( meta.getFieldsName(), "" ) ); //$NON-NLS-1$
    wCollection.setText( Const.NVL( meta.getCollection(), "" ) ); //$NON-NLS-1$
    wJsonField.setText( Const.NVL( meta.getJsonFieldName(), "" ) ); //$NON-NLS-1$
    wJsonQuery.setText( Const.NVL( meta.getJsonQuery(), "" ) ); //$NON-NLS-1$

    wAuthDbName.setText( Const.NVL( meta.getAuthenticationDatabaseName(), "" ) ); // $NON-NLS-1$ //$NON-NLS-1$
    wAuthUser.setText( Const.NVL( meta.getAuthenticationUser(), "" ) ); // $NON-NLS-1$ //$NON-NLS-1$
    wAuthPass.setText( Const.NVL( meta.getAuthenticationPassword(), "" ) ); // $NON-NLS-1$ //$NON-NLS-1$
    m_kerberosBut.setSelection( meta.getUseKerberosAuthentication() );
    wAuthPass.setEnabled( !m_kerberosBut.getSelection() );
    m_connectionTimeout.setText( Const.NVL( meta.getConnectTimeout(), "" ) ); //$NON-NLS-1$
    m_socketTimeout.setText( Const.NVL( meta.getSocketTimeout(), "" ) ); //$NON-NLS-1$
    m_readPreference.setText( Const.NVL( meta.getReadPreference(), "" ) ); //$NON-NLS-1$
    m_queryIsPipelineBut.setSelection( meta.getQueryIsPipeline() );
    m_outputAsJson.setSelection( meta.getOutputJson() );
    m_executeForEachRowBut.setSelection( meta.getExecuteForEachIncomingRow() );

    setFieldTableFields( meta.getMongoFields() );
    setTagsTableFields( meta.getReadPrefTagSets() );

    wJsonField.setEnabled( meta.getOutputJson() );
    wGet.setEnabled( !meta.getOutputJson() );

    updateQueryTitleInfo();

    wStepname.selectAll();
  }

  private void updateQueryTitleInfo() {
    if ( m_queryIsPipelineBut.getSelection() ) {
      wlJsonQuery.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.JsonQuery.Label2" ) //$NON-NLS-1$
          + ": db." //$NON-NLS-1$
          + Const.NVL( wCollection.getText(), "n/a" ) + ".aggregate(..." ); //$NON-NLS-1$ //$NON-NLS-2$
      wFieldsName.setEnabled( false );
    } else {
      wlJsonQuery.setText( BaseMessages.getString( PKG, "MongoDbInputDialog.JsonQuery.Label" ) ); //$NON-NLS-1$
      wFieldsName.setEnabled( true );
    }
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void getInfo( MongoDbInputMeta meta ) {

    meta.setHostnames( wHostname.getText() );
    meta.setPort( wPort.getText() );
    meta.setUseAllReplicaSetMembers( m_useAllReplicaSetMembersBut.getSelection() );
    meta.setDbName( wDbName.getText() );
    meta.setFieldsName( wFieldsName.getText() );
    meta.setCollection( wCollection.getText() );
    meta.setJsonFieldName( wJsonField.getText() );
    meta.setJsonQuery( wJsonQuery.getText() );

    meta.setAuthenticationDatabaseName( wAuthDbName.getText() );
    meta.setAuthenticationUser( wAuthUser.getText() );
    meta.setAuthenticationPassword( wAuthPass.getText() );
    meta.setAuthenticationMechanism( m_dbAuthMec.getText() );
    meta.setUseKerberosAuthentication( m_kerberosBut.getSelection() );
    meta.setConnectTimeout( m_connectionTimeout.getText() );
    meta.setSocketTimeout( m_socketTimeout.getText() );
    meta.setReadPreference( m_readPreference.getText() );
    meta.setOutputJson( m_outputAsJson.getSelection() );
    meta.setQueryIsPipeline( m_queryIsPipelineBut.getSelection() );
    meta.setExecuteForEachIncomingRow( m_executeForEachRowBut.getSelection() );

    int numNonEmpty = m_fieldsView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      List<MongoField> outputFields = new ArrayList<MongoField>();
      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_fieldsView.getNonEmpty( i );
        MongoField newField = new MongoField();

        newField.m_fieldName = item.getText( 1 ).trim();
        newField.m_fieldPath = item.getText( 2 ).trim();
        newField.m_kettleType = item.getText( 3 ).trim();

        if ( !Const.isEmpty( item.getText( 4 ) ) ) {
          newField.m_indexedVals = MongoDbInputData.indexedValsList( item.getText( 4 ).trim() );
        }

        outputFields.add( newField );
      }

      meta.setMongoFields( outputFields );
    }

    numNonEmpty = m_tagsView.nrNonEmpty();

    List<String> tags = new ArrayList<String>();
    if ( numNonEmpty > 0 ) {

      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_tagsView.getNonEmpty( i );
        String t = item.getText( 1 ).trim();
        if ( !t.startsWith( "{" ) ) { //$NON-NLS-1$
          t = "{" + t; //$NON-NLS-1$
        }
        if ( !t.endsWith( "}" ) ) { //$NON-NLS-1$
          t += "}"; //$NON-NLS-1$
        }

        tags.add( t );
      }
    }
    meta.setReadPrefTagSets( tags );
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    getInfo( input );

    dispose();
  }

  public boolean isTableDisposed() {
    return m_fieldsView.isDisposed();
  }

  private void setTagsTableFields( List<String> tags ) {
    if ( tags == null ) {
      return;
    }

    m_tagsView.clearAll();

    for ( String t : tags ) {
      TableItem item = new TableItem( m_tagsView.table, SWT.NONE );
      item.setText( 1, t );
    }

    m_tagsView.removeEmptyRows();
    m_tagsView.setRowNums();
    m_tagsView.optWidth( true );
  }

  private void setFieldTableFields( List<MongoField> fields ) {
    if ( fields == null ) {
      return;
    }

    m_fieldsView.clearAll();
    for ( MongoField f : fields ) {
      TableItem item = new TableItem( m_fieldsView.table, SWT.NONE );

      updateTableItem( item, f );
    }

    m_fieldsView.removeEmptyRows();
    m_fieldsView.setRowNums();
    m_fieldsView.optWidth( true );
  }

  public void updateFieldTableFields( List<MongoField> fields ) {
    Map<String, MongoField> fieldMap = new HashMap<String, MongoField>( fields.size() );
    for ( MongoField field : fields ) {
      fieldMap.put( field.m_fieldName, field );
    }

    int index = 0;
    List<Integer> indicesToRemove = new ArrayList<Integer>();
    for ( TableItem tableItem : m_fieldsView.getTable().getItems() ) {
      String name = tableItem.getText( 1 );
      MongoField mongoField = fieldMap.remove( name );
      if ( mongoField == null ) {
        //Value does not exist in incoming fields list and exists in table, remove old value from table
        indicesToRemove.add( index );
      } else {
        //Value exists in incoming fields list and in table, update entry
        updateTableItem( tableItem, mongoField );
      }
      index++;
    }

    int[] indicesArray = new int[indicesToRemove.size()];
    for ( int i = 0; i < indicesArray.length; i++ ) {
      indicesArray[i] = indicesToRemove.get( i );
    }

    for ( MongoField mongoField : fieldMap.values() ) {
      TableItem item = new TableItem( m_fieldsView.table, SWT.NONE );
      updateTableItem( item, mongoField );
    }
    m_fieldsView.setRowNums();
    m_fieldsView.remove( indicesArray );
    m_fieldsView.removeEmptyRows();
    m_fieldsView.setRowNums();
    m_fieldsView.optWidth( true );
  }

  private void updateTableItem( TableItem tableItem, MongoField mongoField ) {
    if ( !Const.isEmpty( mongoField.m_fieldName ) ) {
      tableItem.setText( 1, mongoField.m_fieldName );
    }

    if ( !Const.isEmpty( mongoField.m_fieldPath ) ) {
      tableItem.setText( 2, mongoField.m_fieldPath );
    }

    if ( !Const.isEmpty( mongoField.m_kettleType ) ) {
      tableItem.setText( 3, mongoField.m_kettleType );
    }

    if ( mongoField.m_indexedVals != null && mongoField.m_indexedVals.size() > 0 ) {
      tableItem.setText( 4, MongoDbInputData.indexedValsList( mongoField.m_indexedVals ) );
    }

    if ( !Const.isEmpty( mongoField.m_arrayIndexInfo ) ) {
      tableItem.setText( 5, mongoField.m_arrayIndexInfo );
    }

    if ( !Const.isEmpty( mongoField.m_occurenceFraction ) ) {
      tableItem.setText( 6, mongoField.m_occurenceFraction );
    }

    if ( mongoField.m_disparateTypes ) {
      tableItem.setText( 7, "Y" ); //$NON-NLS-1$
    }
  }

  private boolean checkForUnresolved( MongoDbInputMeta meta, String title ) {

    String query = transMeta.environmentSubstitute( meta.getJsonQuery() );

    boolean notOk = ( query.contains( "${" ) || query.contains( "?{" ) ); //$NON-NLS-1$ //$NON-NLS-2$

    if ( notOk ) {
      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK, title, BaseMessages.getString( PKG,
              "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs" ) ); //$NON-NLS-1$
      smd.open();
    }

    return !notOk;
  }

  //Used to catch exceptions from discoverFields calls that come through the callback
  public void handleNotificationException( Exception exception ) {
    new ErrorDialog( shell, stepname,
        BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling" ), exception ); //$NON-NLS-1$
  }

  private void getFields( MongoDbInputMeta meta ) {
    if ( !Const.isEmpty( wHostname.getText() ) && !Const.isEmpty( wDbName.getText() ) && !Const
        .isEmpty( wCollection.getText() ) ) {
      EnterNumberDialog
          end =
          new EnterNumberDialog( shell, 100, BaseMessages.getString( PKG, "MongoDbInputDialog.SampleDocuments.Title" ),
              //$NON-NLS-1$
              BaseMessages.getString( PKG, "MongoDbInputDialog.SampleDocuments.Message" ) ); //$NON-NLS-1$
      int samples = end.open();
      if ( samples > 0 ) {

        getInfo( meta );
        // Turn off execute for each incoming row (if set).
        // Query is still going to
        // be stuffed if the user has specified field replacement (i.e.
        // ?{...}) in the query string
        boolean current = meta.getExecuteForEachIncomingRow();
        meta.setExecuteForEachIncomingRow( false );

        if ( !checkForUnresolved( meta, BaseMessages.getString( PKG,
            "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.SamplingTitle" ) ) ) {
          //$NON-NLS-1$
          return;
        }

        try {
          discoverFields( meta, transMeta, samples, this );
          meta.setExecuteForEachIncomingRow( current );
        } catch ( KettleException e ) {
          new ErrorDialog( shell, stepname,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.ErrorDuringSampling" ), e ); //$NON-NLS-1$
        }
      }
    } else {
      // pop up an error dialog

      String missingConDetails = "";
      if ( Const.isEmpty( wHostname.getText() ) ) {
        missingConDetails += " host name(s)";
      }
      if ( Const.isEmpty( wDbName.getText() ) ) {
        missingConDetails += " database";
      }
      if ( Const.isEmpty( wCollection.getText() ) ) {
        missingConDetails += " collection";
      }

      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                  missingConDetails ) ); //$NON-NLS-1$
      smd.open();
    }
  }

  // Preview the data
  private void preview() {
    // Create the XML input step
    MongoDbInputMeta oneMeta = new MongoDbInputMeta();
    getInfo( oneMeta );

    // Turn off execute for each incoming row (if set). Query is still going to
    // be stuffed if the user has specified field replacement (i.e. ?{...}) in
    // the query string
    oneMeta.setExecuteForEachIncomingRow( false );

    if ( !checkForUnresolved( oneMeta, BaseMessages.getString( PKG,
        "MongoDbInputDialog.Warning.Message.MongoQueryContainsUnresolvedVarsFieldSubs.PreviewTitle" ) ) ) { //$NON-NLS-1$
      return;
    }

    TransMeta
        previewMeta =
        TransPreviewFactory.generatePreviewTransformation( transMeta, oneMeta, wStepname.getText() );

    EnterNumberDialog
        numberDialog =
        new EnterNumberDialog( shell, props.getDefaultPreviewSize(),
            BaseMessages.getString( PKG, "MongoDbInputDialog.PreviewSize.DialogTitle" ), //$NON-NLS-1$
            BaseMessages.getString( PKG, "MongoDbInputDialog.PreviewSize.DialogMessage" ) ); //$NON-NLS-1$
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog
          progressDialog =
          new TransPreviewProgressDialog( shell, previewMeta, new String[] { wStepname.getText() },
              new int[] { previewSize } );
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog
              etd =
              new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
                  //$NON-NLS-1$
                  BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), //$NON-NLS-1$
                  loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog
          prd =
          new PreviewRowsDialog( shell, transMeta, SWT.NONE, wStepname.getText(),
              progressDialog.getPreviewRowsMeta( wStepname.getText() ),
              progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
      prd.open();
    }
  }

  private void setupDBNames() {
    String current = wDbName.getText();
    wDbName.removeAll();

    String hostname = transMeta.environmentSubstitute( wHostname.getText() );

    if ( !Const.isEmpty( hostname ) ) {

      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo( meta );
      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        List<String> dbNames = new ArrayList<String>();
        try {
          dbNames = wrapper.getDatabaseNames();
        } finally {
          wrapper.dispose();
        }

        for ( String s : dbNames ) {
          wDbName.add( s );
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage." + "UnableToConnect" ),
            //$NON-NLS-1$ //$NON-NLS-2$
            BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      // popup some feedback
      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                  "host name(s)" ) ); //$NON-NLS-1$
      smd.open();
    }

    if ( !Const.isEmpty( current ) ) {
      wDbName.setText( current );
    }
  }

  private void setupCollectionNamesForDB() {
    final String hostname = transMeta.environmentSubstitute( wHostname.getText() );
    final String dB = transMeta.environmentSubstitute( wDbName.getText() );

    String current = wCollection.getText();
    wCollection.removeAll();

    if ( !Const.isEmpty( hostname ) && !Const.isEmpty( dB ) ) {

      final MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo( meta );
      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        Set<String> collections = new HashSet<String>();
        try {
          collections = wrapper.getCollectionsNames( dB );
        } finally {
          wrapper.dispose();
        }

        for ( String c : collections ) {
          wCollection.add( c );
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage." + "UnableToConnect" ),
            //$NON-NLS-1$ //$NON-NLS-2$
            BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      // popup some feedback

      String missingConnDetails = "";
      if ( Const.isEmpty( hostname ) ) {
        missingConnDetails += "host name(s)";
      }
      if ( Const.isEmpty( dB ) ) {
        missingConnDetails += " database";
      }
      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                  missingConnDetails ) ); //$NON-NLS-1$
      smd.open();
    }

    if ( !Const.isEmpty( current ) ) {
      wCollection.setText( current );
    }
  }

  private void setupTagSetComboValues() {
    String hostname = transMeta.environmentSubstitute( wHostname.getText() );

    if ( !Const.isEmpty( hostname ) ) {
      MongoDbInputMeta meta = new MongoDbInputMeta();
      getInfo( meta );

      try {
        MongoClientWrapper wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
        List<String> repSetTags = new ArrayList<String>();
        try {
          repSetTags = wrapper.getAllTags();
        } finally {
          wrapper.dispose();
        }

        if ( repSetTags.size() == 0 ) {
          ShowMessageDialog
              smd =
              new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
                  BaseMessages.getString( PKG, "MongoDbInputDialog.Info.Message.NoTagSetsDefinedOnServer" ),
                  BaseMessages
                      .getString( PKG, "MongoDbInputDialog.Info.Message.NoTagSetsDefinedOnServer" ) ); //$NON-NLS-1$
          smd.open();
        } else {
          this.setTagsTableFields( repSetTags );
        }
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
        new ErrorDialog( shell, BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage." + "UnableToConnect" ),
            //$NON-NLS-1$ //$NON-NLS-2$
            BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), e ); //$NON-NLS-1$
      }
    } else {
      // popup some feedback
      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_WARNING | SWT.OK,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails.Title" ),
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.MissingConnectionDetails",
                  "host name(s)" ) ); //$NON-NLS-1$
      smd.open();
    }
  }
  /* Only referenced in commented code, commenting out also
  private void deleteSelectedFromView() {
    if (m_tagsView.nrNonEmpty() > 0 && m_tagsView.getSelectionIndex() >= 0) {
      int selectedI = m_tagsView.getSelectionIndex();

      m_tagsView.remove(selectedI);
      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
    }
  }

  private void moveSelectedTagSetToEditor() {
    if (m_tagsView.nrNonEmpty() > 0 && m_tagsView.getSelectionIndex() >= 0) {
      int selectedI = m_tagsView.getSelectionIndex();

      String selected = m_tagsView.getItem(selectedI)[0];
      if (selected.startsWith("{")) { //$NON-NLS-1$
        selected = selected.substring(1);
      }
      if (selected.endsWith("}")) { //$NON-NLS-1$
        selected = selected.substring(0, selected.length() - 1);
      }

      m_tagsCombo.setText(selected);
      m_currentTagsState = selected;

      m_tagsView.remove(selectedI);
      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
    }
  } 
  
  private void addTagsToTable() {
    if (!Const.isEmpty(m_tagsCombo.getText())) {
      TableItem item = new TableItem(m_tagsView.table, SWT.NONE);
      String tagSet = m_tagsCombo.getText();
      if (!tagSet.startsWith("{")) { //$NON-NLS-1$
        tagSet = "{" + tagSet; //$NON-NLS-1$
      }
      if (!tagSet.endsWith("}")) { //$NON-NLS-1$
        tagSet = tagSet + "}"; //$NON-NLS-1$
      }
      item.setText(1, tagSet);

      m_tagsView.removeEmptyRows();
      m_tagsView.setRowNums();
      m_tagsView.optWidth(true);

      m_currentTagsState = ""; //$NON-NLS-1$
      m_tagsCombo.setText(""); //$NON-NLS-1$
    }
  }*/

  private void testUserSpecifiedTagSetsAgainstReplicaSet() {
    if ( m_tagsView.nrNonEmpty() > 0 ) {
      List<DBObject> tagSets = new ArrayList<DBObject>();

      for ( int i = 0; i < m_tagsView.nrNonEmpty(); i++ ) {
        TableItem item = m_tagsView.getNonEmpty( i );

        String set = item.getText( 1 ).trim();
        if ( !set.startsWith( "{" ) ) { //$NON-NLS-1$
          set = "{" + set; //$NON-NLS-1$
        }

        if ( !set.endsWith( "}" ) ) { //$NON-NLS-1$
          set = set + "}"; //$NON-NLS-1$
        }

        DBObject setO = (DBObject) JSON.parse( set );
        if ( setO != null ) {
          tagSets.add( setO );
        }
      }

      if ( tagSets.size() > 0 ) {
        String hostname = transMeta.environmentSubstitute( wHostname.getText() );
        try {
          if ( !Const.isEmpty( hostname ) ) {
            MongoDbInputMeta meta = new MongoDbInputMeta();
            getInfo( meta );
            MongoClientWrapper wrapper = null;
            try {
              wrapper = MongoWrapperUtil.createMongoClientWrapper( meta, transMeta, log );
            } catch ( MongoDbException e ) {
              throw new KettleException( e );
            }
            List<String> satisfy = new ArrayList<String>();
            try {
              try {
                satisfy = wrapper.getReplicaSetMembersThatSatisfyTagSets( tagSets );
              } catch ( MongoDbException e ) {
                throw new KettleException( e );
              }
            } finally {
              try {
                wrapper.dispose();
              } catch ( MongoDbException e ) {
                //Ignore
              }
            }

            if ( satisfy.size() == 0 ) {
              logBasic( BaseMessages
                  .getString( PKG, "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets" ) ); //$NON-NLS-1$
              ShowMessageDialog
                  smd =
                  new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK, BaseMessages
                      .getString( PKG, "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets.Title" ),
                      //$NON-NLS-1$
                      BaseMessages.getString( PKG,
                          "MongoDbInputDialog.Info.Message.NoReplicaSetMembersMatchTagSets" ) ); //$NON-NLS-1$
              smd.open();
            } else {
              StringBuilder builder = new StringBuilder();
              builder.append( "\n" ); //$NON-NLS-1$
              for ( int i = 0; i < satisfy.size(); i++ ) {
                builder.append( satisfy.get( i ) ).append( "\n" ); //$NON-NLS-1$
              }

              ShowMessageDialog
                  smd =
                  new ShowMessageDialog( shell, SWT.ICON_INFORMATION | SWT.OK,
                      BaseMessages.getString( PKG, "MongoDbInputDialog.Info.Message.MatchingReplicaSetMembers.Title" ),
                      //$NON-NLS-1$
                      builder.toString() );
              smd.open();
            }
          } else {
            // popup dialog saying that no connection details are available
            ShowMessageDialog
                smd =
                new ShowMessageDialog( shell, SWT.ICON_ERROR | SWT.OK,
                    BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoConnectionDetailsSupplied.Title" ),
                    //$NON-NLS-1$
                    BaseMessages.getString( PKG,
                        "MongoDbInputDialog.ErrorMessage.NoConnectionDetailsSupplied" ) ); //$NON-NLS-1$
            smd.open();
          }

        } catch ( KettleException ex ) {
          // popup an error dialog
          logError( BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ),
              ex ); //$NON-NLS-1$
          new ErrorDialog( shell, BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage." + "UnableToConnect" ),
              //$NON-NLS-1$ //$NON-NLS-2$
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.UnableToConnect" ), ex ); //$NON-NLS-1$
        }
      } else {
        // popup a dialog stating that there were no parseable tag sets
        ShowMessageDialog
            smd =
            new ShowMessageDialog( shell, SWT.ICON_ERROR | SWT.OK,
                BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets.Title" ), //$NON-NLS-1$
                BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoParseableTagSets" ) ); //$NON-NLS-1$
        smd.open();
      }
    } else {
      // popup a dialog saying that there are no tag sets defined
      ShowMessageDialog
          smd =
          new ShowMessageDialog( shell, SWT.ICON_ERROR | SWT.OK,
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoTagSetsDefined.Title" ), //$NON-NLS-1$
              BaseMessages.getString( PKG, "MongoDbInputDialog.ErrorMessage.NoTagSetsDefined" ) ); //$NON-NLS-1$
      smd.open();
    }
  }

  private void concatenateTags() {
    int[] selectedTags = this.m_tagsView.getSelectionIndices();
    String concatenated = "";

    for ( int i : selectedTags ) {
      TableItem item = m_tagsView.table.getItem( i );
      String t = item.getText( 1 ).trim();
      concatenated =
          concatenated + ( ( concatenated.length() > 0 ) ? ( ( !concatenated.endsWith( "," ) ) ? ", " : "" ) : "" )
              + t; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }
    TableItem item = new TableItem( m_tagsView.table, SWT.NONE );
    item.setText( 1, concatenated );

  }


  public static void discoverFields( final MongoDbInputMeta meta, final VariableSpace vars, final int docsToSample,
                                     final MongoDbInputDialog mongoDialog ) throws KettleException {
    MongoProperties.Builder propertiesBuilder = MongoWrapperUtil.createPropertiesBuilder( meta, vars );
    String db = vars.environmentSubstitute( meta.getDbName() );
    String collection = vars.environmentSubstitute( meta.getCollection() );
    String query = vars.environmentSubstitute( meta.getJsonQuery() );
    String fields = vars.environmentSubstitute( meta.getFieldsName() );
    int numDocsToSample = docsToSample;
    if ( numDocsToSample < 1 ) {
      numDocsToSample = 100; // default
    }
    try {
      MongoDbInputData.getMongoDbInputDiscoverFieldsHolder().getMongoDbInputDiscoverFields()
        .discoverFields( propertiesBuilder, db, collection, query, fields, meta.getQueryIsPipeline(), numDocsToSample,
          meta, new DiscoverFieldsCallback() {
            @Override public void notifyFields( final List<MongoField> fields ) {
              if ( fields.size() > 0 ) {
                Spoon.getInstance().getDisplay().asyncExec( new Runnable() {
                  @Override public void run() {
                    if ( !mongoDialog.isTableDisposed() ) {
                      meta.setMongoFields( fields );
                      mongoDialog.updateFieldTableFields( meta.getMongoFields() );
                    }
                  }
                } );
              }
            }

            @Override public void notifyException( Exception exception ) {
              mongoDialog.handleNotificationException( exception );
            }
          } );
    } catch ( KettleException e ) {
      throw new KettleException( "Unable to discover fields from MongoDB", e );
    }
  }

  public static boolean discoverFields( final MongoDbInputMeta meta, final VariableSpace vars, final int docsToSample )
    throws KettleException {

    MongoProperties.Builder propertiesBuilder = MongoWrapperUtil.createPropertiesBuilder( meta, vars );
    try {
      String db = vars.environmentSubstitute( meta.getDbName() );
      String collection = vars.environmentSubstitute( meta.getCollection() );
      String query = vars.environmentSubstitute( meta.getJsonQuery() );
      String fields = vars.environmentSubstitute( meta.getFieldsName() );
      int numDocsToSample = docsToSample;
      if ( numDocsToSample < 1 ) {
        numDocsToSample = 100; // default
      }
      List<MongoField> discoveredFields =
        MongoDbInputData.getMongoDbInputDiscoverFieldsHolder().getMongoDbInputDiscoverFields().discoverFields(
          propertiesBuilder, db, collection, query, fields, meta.getQueryIsPipeline(), numDocsToSample, meta );

      // return true if query resulted in documents being returned and fields
      // getting extracted
      if ( discoveredFields.size() > 0 ) {
        meta.setMongoFields( discoveredFields );
        return true;
      }
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( "Unable to discover fields from MongoDB", e );
      }
    }
    return false;
  }

}
