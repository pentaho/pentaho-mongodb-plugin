/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
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

package org.pentaho.di.trans.steps.mongodbdelete;

import java.security.PrivilegedActionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.ShowMessageDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.mongo.NamedReadPreference;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoClientWrapperFactory;

import com.mongodb.DBObject;

/**
 * Dialog class for MongoDbDelete step
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
public class MongoDbDeleteDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = MongoDbDeleteMeta.class;
    protected MongoDbDeleteMeta m_currentMeta;
    protected MongoDbDeleteMeta m_originalMeta;
    /** various UI bits and pieces for the dialog */
    private Label m_stepnameLabel;
    private Text m_stepnameText;
    // The tabs of the dialog
    private CTabFolder m_wTabFolder;
    private CTabItem m_wConfigTab;
    private CTabItem m_wOutputOptionsTab;
    private CTabItem m_wMongoFieldsTab;
    private Button m_getFieldsBut;
    private Button m_previewDocStructBut;
    private TextVar m_hostnameField;
    private TextVar m_portField;
    private Button m_useAllReplicaSetMembersBut;
    private TextVar m_usernameField;
    private TextVar m_passField;
    private Button m_kerberosBut;
    private TextVar m_connectTimeout;
    private TextVar m_socketTimeout;
    private CCombo m_dbNameField;
    private Button m_getDBsBut;
    private CCombo m_collectionField;
    private Button m_getCollectionsBut;
    private CCombo m_writeConcern;
    private TextVar m_wTimeout;
    private Button m_journalWritesCheck;
    private CCombo m_readPreference;
    private TextVar m_writeRetries;
    private TextVar m_writeRetryDelay;
    private TableView m_mongoFieldsView;
    private ColumnInfo[] colInf;
    private Map<String, Integer> inputFields;

    public MongoDbDeleteDialog(Shell parent, Object in, TransMeta tr, String name) {

        super(parent, (BaseStepMeta) in, tr, name);

        m_currentMeta = (MongoDbDeleteMeta) in;
        m_originalMeta = (MongoDbDeleteMeta) m_currentMeta.clone();

        inputFields = new HashMap<String, Integer>();
    }

    @Override
    public String open() {

        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);

        props.setLook(shell);
        setShellImage(shell, m_currentMeta);

        // used to listen to a text field (m_wStepname)
        ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_currentMeta.setChanged();
            }
        };

        changed = m_currentMeta.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        m_stepnameLabel = new Label(shell, SWT.RIGHT);
        m_stepnameLabel.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.StepName.Label"));
        props.setLook(m_stepnameLabel);

        FormData fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -margin);
        fd.top = new FormAttachment(0, margin);
        m_stepnameLabel.setLayoutData(fd);
        m_stepnameText = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        m_stepnameText.setText(stepname);
        props.setLook(m_stepnameText);
        m_stepnameText.addModifyListener(lsMod);

        // format the text field
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(100, 0);
        m_stepnameText.setLayoutData(fd);

        m_wTabFolder = new CTabFolder(shell, SWT.BORDER);
        props.setLook(m_wTabFolder, Props.WIDGET_STYLE_TAB);
        m_wTabFolder.setSimple(false);

        // Start of the config tab
        m_wConfigTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wConfigTab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ConfigTab.TabTitle")); //$NON-NLS-1$

        Composite wConfigComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wConfigComp);

        FormLayout configLayout = new FormLayout();
        configLayout.marginWidth = 3;
        configLayout.marginHeight = 3;
        wConfigComp.setLayout(configLayout);

        // hostname line
        Label hostnameLab = new Label(wConfigComp, SWT.RIGHT);
        hostnameLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Hostname.Label")); //$NON-NLS-1$
        hostnameLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Hostname.TipText")); //$NON-NLS-1$
        props.setLook(hostnameLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(middle, -margin);
        hostnameLab.setLayoutData(fd);

        m_hostnameField = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_hostnameField);
        m_hostnameField.addModifyListener(lsMod);
        // set the tool tip to the contents with any env variables expanded
        m_hostnameField.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_hostnameField.setToolTipText(transMeta.environmentSubstitute(m_hostnameField.getText()));
            }
        });
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(0, 0);
        fd.left = new FormAttachment(middle, 0);
        m_hostnameField.setLayoutData(fd);

        // port line
        Label portLab = new Label(wConfigComp, SWT.RIGHT);
        portLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Port.Label")); //$NON-NLS-1$
        portLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Port.Label.TipText")); //$NON-NLS-1$
        props.setLook(portLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_hostnameField, margin);
        fd.right = new FormAttachment(middle, -margin);
        portLab.setLayoutData(fd);

        m_portField = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_portField);
        m_portField.addModifyListener(lsMod);
        // set the tool tip to the contents with any env variables expanded
        m_portField.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_portField.setToolTipText(transMeta.environmentSubstitute(m_portField.getText()));
            }
        });
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_hostnameField, margin);
        fd.left = new FormAttachment(middle, 0);
        m_portField.setLayoutData(fd);

        // Use all replica set members check box
        Label useAllReplicaLab = new Label(wConfigComp, SWT.RIGHT);
        useAllReplicaLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.UseAllReplicaSetMembers.Label")); //$NON-NLS-1$
        useAllReplicaLab.setToolTipText(BaseMessages
                .getString(PKG, "MongoDbDeleteDialog.UseAllReplicaSetMembers.TipText")); //$NON-NLS-1$
        props.setLook(useAllReplicaLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(middle, -margin);
        fd.top = new FormAttachment(m_portField, margin);
        useAllReplicaLab.setLayoutData(fd);

        m_useAllReplicaSetMembersBut = new Button(wConfigComp, SWT.CHECK);
        props.setLook(m_useAllReplicaSetMembersBut);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_portField, margin);
        m_useAllReplicaSetMembersBut.setLayoutData(fd);

        // username field
        Label userLab = new Label(wConfigComp, SWT.RIGHT);
        userLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Username.Label")); //$NON-NLS-1$
        props.setLook(userLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_useAllReplicaSetMembersBut, margin);
        fd.right = new FormAttachment(middle, -margin);
        userLab.setLayoutData(fd);

        m_usernameField = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_usernameField);
        m_usernameField.addModifyListener(lsMod);
        // set the tool tip to the contents with any env variables expanded
        m_usernameField.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_usernameField.setToolTipText(transMeta.environmentSubstitute(m_usernameField.getText()));
            }
        });
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_useAllReplicaSetMembersBut, margin);
        fd.left = new FormAttachment(middle, 0);
        m_usernameField.setLayoutData(fd);

        // password field
        Label passLab = new Label(wConfigComp, SWT.RIGHT);
        passLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Password.Label")); //$NON-NLS-1$
        props.setLook(passLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_usernameField, margin);
        fd.right = new FormAttachment(middle, -margin);
        passLab.setLayoutData(fd);

        m_passField = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_passField);
        m_passField.setEchoChar('*');
        // If the password contains a variable, don't hide it.
        m_passField.getTextWidget().addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                checkPasswordVisible();
            }
        });

        m_passField.addModifyListener(lsMod);

        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_usernameField, margin);
        fd.left = new FormAttachment(middle, 0);
        m_passField.setLayoutData(fd);

        // use kerberos authentication
        Label kerbLab = new Label(wConfigComp, SWT.RIGHT);
        kerbLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Kerberos.Label")); //$NON-NLS-1$
        props.setLook(kerbLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_passField, margin);
        fd.right = new FormAttachment(middle, -margin);
        kerbLab.setLayoutData(fd);

        m_kerberosBut = new Button(wConfigComp, SWT.CHECK);
        props.setLook(m_kerberosBut);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_passField, margin);
        m_kerberosBut.setLayoutData(fd);

        m_kerberosBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                m_passField.setEnabled(!m_kerberosBut.getSelection());
            }
        });

        // connection timeout
        Label connectTimeoutL = new Label(wConfigComp, SWT.RIGHT);
        connectTimeoutL.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ConnectionTimeout.Label")); //$NON-NLS-1$
        props.setLook(connectTimeoutL);
        connectTimeoutL.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ConnectionTimeout.TipText")); //$NON-NLS-1$

        fd = new FormData();
        fd.left = new FormAttachment(0, -margin);
        fd.top = new FormAttachment(m_kerberosBut, margin);
        fd.right = new FormAttachment(middle, -margin);
        connectTimeoutL.setLayoutData(fd);

        m_connectTimeout = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_connectTimeout);
        m_connectTimeout.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_kerberosBut, margin);
        fd.right = new FormAttachment(100, 0);
        m_connectTimeout.setLayoutData(fd);
        m_connectTimeout.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_connectTimeout.setToolTipText(transMeta.environmentSubstitute(m_connectTimeout.getText()));
            }
        });

        // socket timeout
        Label socketTimeoutL = new Label(wConfigComp, SWT.RIGHT);
        socketTimeoutL.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.SocketTimeout.Label")); //$NON-NLS-1$
        props.setLook(connectTimeoutL);
        socketTimeoutL.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.SocketTimeout.TipText")); //$NON-NLS-1$

        fd = new FormData();
        fd.left = new FormAttachment(0, -margin);
        fd.top = new FormAttachment(m_connectTimeout, margin);
        fd.right = new FormAttachment(middle, -margin);
        socketTimeoutL.setLayoutData(fd);

        m_socketTimeout = new TextVar(transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_socketTimeout);
        m_socketTimeout.addModifyListener(lsMod);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_connectTimeout, margin);
        fd.right = new FormAttachment(100, 0);
        m_socketTimeout.setLayoutData(fd);
        m_socketTimeout.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_socketTimeout.setToolTipText(transMeta.environmentSubstitute(m_socketTimeout.getText()));
            }
        });

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, 0);
        wConfigComp.setLayoutData(fd);

        wConfigComp.layout();
        m_wConfigTab.setControl(wConfigComp);

        // --- start of the options tab
        m_wOutputOptionsTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wOutputOptionsTab.setText("Output options"); //$NON-NLS-1$
        Composite wOutputComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wOutputComp);
        FormLayout outputLayout = new FormLayout();
        outputLayout.marginWidth = 3;
        outputLayout.marginHeight = 3;
        wOutputComp.setLayout(outputLayout);

        // DB name
        Label dbNameLab = new Label(wOutputComp, SWT.RIGHT);
        dbNameLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.DBName.Label")); //$NON-NLS-1$
        dbNameLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.DBName.TipText")); //$NON-NLS-1$
        props.setLook(dbNameLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, margin);
        fd.right = new FormAttachment(middle, -margin);
        dbNameLab.setLayoutData(fd);

        m_getDBsBut = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
        props.setLook(m_getDBsBut);
        m_getDBsBut.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.GetDBs.Button")); //$NON-NLS-1$
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(0, 0);
        m_getDBsBut.setLayoutData(fd);

        m_dbNameField = new CCombo(wOutputComp, SWT.BORDER);
        props.setLook(m_dbNameField);

        m_dbNameField.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_currentMeta.setChanged();
                m_dbNameField.setToolTipText(transMeta.environmentSubstitute(m_dbNameField.getText()));
            }
        });

        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_passField, margin);
        fd.right = new FormAttachment(m_getDBsBut, -margin);
        m_dbNameField.setLayoutData(fd);

        m_getDBsBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                setupDBNames();
            }
        });

        // collection line
        Label collectionLab = new Label(wOutputComp, SWT.RIGHT);
        collectionLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Collection.Label")); //$NON-NLS-1$
        collectionLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Collection.TipText")); //$NON-NLS-1$
        props.setLook(collectionLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_dbNameField, margin);
        fd.right = new FormAttachment(middle, -margin);
        collectionLab.setLayoutData(fd);

        m_getCollectionsBut = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
        props.setLook(m_getCollectionsBut);
        m_getCollectionsBut.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.GetCollections.Button")); //$NON-NLS-1$
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_dbNameField, 0);
        m_getCollectionsBut.setLayoutData(fd);

        m_getCollectionsBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                // setupMappingNamesForTable(false);
                setupCollectionNamesForDB(false);
            }
        });

        m_collectionField = new CCombo(wOutputComp, SWT.BORDER);
        props.setLook(m_collectionField);
        m_collectionField.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_currentMeta.setChanged();

                m_collectionField.setToolTipText(transMeta.environmentSubstitute(m_collectionField.getText()));
            }
        });
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_dbNameField, margin);
        fd.right = new FormAttachment(m_getCollectionsBut, -margin);
        m_collectionField.setLayoutData(fd);

        // write concern
        Label writeConcernLab = new Label(wOutputComp, SWT.RIGHT);
        writeConcernLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteConcern.Label")); //$NON-NLS-1$
        writeConcernLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteConcern.TipText")); //$NON-NLS-1$
        props.setLook(writeConcernLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_collectionField, margin);
        fd.right = new FormAttachment(middle, -margin);
        writeConcernLab.setLayoutData(fd);

        Button getCustomWCBut = new Button(wOutputComp, SWT.PUSH | SWT.CENTER);
        props.setLook(getCustomWCBut);
        getCustomWCBut.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteConcern.CustomWriteConcerns")); //$NON-NLS-1$
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_collectionField, 0);
        getCustomWCBut.setLayoutData(fd);

        getCustomWCBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                setupCustomWriteConcernNames();
            }
        });

        m_writeConcern = new CCombo(wOutputComp, SWT.BORDER);
        props.setLook(m_writeConcern);
        m_writeConcern.addModifyListener(lsMod);
        fd = new FormData();
        fd.right = new FormAttachment(getCustomWCBut, 0);
        fd.top = new FormAttachment(m_collectionField, margin);
        fd.left = new FormAttachment(middle, 0);
        m_writeConcern.setLayoutData(fd);

        // wTimeout
        Label wTimeoutLab = new Label(wOutputComp, SWT.RIGHT);
        wTimeoutLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WTimeout.Label")); //$NON-NLS-1$
        wTimeoutLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WTimeout.TipText")); //$NON-NLS-1$
        props.setLook(wTimeoutLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_writeConcern, margin);
        fd.right = new FormAttachment(middle, -margin);
        wTimeoutLab.setLayoutData(fd);

        m_wTimeout = new TextVar(transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_wTimeout);
        m_wTimeout.addModifyListener(lsMod);
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_writeConcern, margin);
        fd.left = new FormAttachment(middle, 0);
        m_wTimeout.setLayoutData(fd);
        m_wTimeout.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_wTimeout.setToolTipText(transMeta.environmentSubstitute(m_wTimeout.getText()));
            }
        });

        Label journalWritesLab = new Label(wOutputComp, SWT.RIGHT);
        journalWritesLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.JournalWrites.Label")); //$NON-NLS-1$
        journalWritesLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.JournalWrites.TipText")); //$NON-NLS-1$
        props.setLook(journalWritesLab);
        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_wTimeout, margin);
        fd.right = new FormAttachment(middle, -margin);
        journalWritesLab.setLayoutData(fd);

        m_journalWritesCheck = new Button(wOutputComp, SWT.CHECK);
        props.setLook(m_journalWritesCheck);
        m_journalWritesCheck.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                m_currentMeta.setChanged();
            }
        });
        fd = new FormData();
        fd.right = new FormAttachment(100, 0);
        fd.top = new FormAttachment(m_wTimeout, margin);
        fd.left = new FormAttachment(middle, 0);
        m_journalWritesCheck.setLayoutData(fd);

        // read preference
        Label readPrefL = new Label(wOutputComp, SWT.RIGHT);
        readPrefL.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ReadPreferenceLabel")); //$NON-NLS-1$
        readPrefL.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ReadPreferenceLabel.TipText")); //$NON-NLS-1$
        props.setLook(readPrefL);
        fd = new FormData();
        fd.left = new FormAttachment(0, -margin);
        fd.top = new FormAttachment(m_journalWritesCheck, margin);
        fd.right = new FormAttachment(middle, -margin);
        readPrefL.setLayoutData(fd);

        m_readPreference = new CCombo(wOutputComp, SWT.BORDER);
        props.setLook(m_readPreference);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_journalWritesCheck, margin);
        fd.right = new FormAttachment(100, 0);
        m_readPreference.setLayoutData(fd);
        m_readPreference.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_currentMeta.setChanged();
                m_readPreference.setToolTipText(transMeta.environmentSubstitute(m_readPreference.getText()));
            }
        });
        m_readPreference.add(NamedReadPreference.PRIMARY.getName());
        m_readPreference.add(NamedReadPreference.PRIMARY_PREFERRED.getName());
        m_readPreference.add(NamedReadPreference.SECONDARY.getName());
        m_readPreference.add(NamedReadPreference.SECONDARY_PREFERRED.getName());
        m_readPreference.add(NamedReadPreference.NEAREST.getName());

        // retries stuff
        Label retriesLab = new Label(wOutputComp, SWT.RIGHT);
        props.setLook(retriesLab);
        retriesLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetries.Label")); //$NON-NLS-1$
        retriesLab.setToolTipText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetries.TipText")); //$NON-NLS-1$
        fd = new FormData();
        fd.left = new FormAttachment(0, -margin);
        fd.top = new FormAttachment(m_readPreference, margin);
        fd.right = new FormAttachment(middle, -margin);
        retriesLab.setLayoutData(fd);

        m_writeRetries = new TextVar(transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_writeRetries);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_readPreference, margin);
        fd.right = new FormAttachment(100, 0);
        m_writeRetries.setLayoutData(fd);
        m_writeRetries.addModifyListener(new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                m_writeRetries.setToolTipText(transMeta.environmentSubstitute(m_writeRetries.getText()));
            }
        });

        Label retriesDelayLab = new Label(wOutputComp, SWT.RIGHT);
        props.setLook(retriesDelayLab);
        retriesDelayLab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.WriteRetriesDelay.Label")); //$NON-NLS-1$
        fd = new FormData();
        fd.left = new FormAttachment(0, -margin);
        fd.top = new FormAttachment(m_writeRetries, margin);
        fd.right = new FormAttachment(middle, -margin);
        retriesDelayLab.setLayoutData(fd);

        m_writeRetryDelay = new TextVar(transMeta, wOutputComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        props.setLook(m_writeRetryDelay);
        fd = new FormData();
        fd.left = new FormAttachment(middle, 0);
        fd.top = new FormAttachment(m_writeRetries, margin);
        fd.right = new FormAttachment(100, 0);
        m_writeRetryDelay.setLayoutData(fd);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, 0);
        wOutputComp.setLayoutData(fd);

        wOutputComp.layout();
        m_wOutputOptionsTab.setControl(wOutputComp);

        // --- start of the fields tab
        m_wMongoFieldsTab = new CTabItem(m_wTabFolder, SWT.NONE);
        m_wMongoFieldsTab.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.FieldsTab.TabTitle")); //$NON-NLS-1$
        Composite wFieldsComp = new Composite(m_wTabFolder, SWT.NONE);
        props.setLook(wFieldsComp);
        FormLayout filterLayout = new FormLayout();
        filterLayout.marginWidth = 3;
        filterLayout.marginHeight = 3;
        wFieldsComp.setLayout(filterLayout);

        colInf = new ColumnInfo[]{
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Path"), ColumnInfo.COLUMN_TYPE_TEXT, false),
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Comparator"), ColumnInfo.COLUMN_TYPE_CCOMBO, false),
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Incoming1"), ColumnInfo.COLUMN_TYPE_CCOMBO, false),
            new ColumnInfo(BaseMessages.getString(PKG, "MongoDbDeleteDialog.Fields.Incoming2"), ColumnInfo.COLUMN_TYPE_CCOMBO, false)};

        colInf[1].setComboValues(Comparator.asLabel());
        colInf[1].setReadOnly(true);

        // Search the fields in the background
        final Runnable runnable = new Runnable() {
            public void run() {
                StepMeta stepMeta = transMeta.findStep(stepname);
                if (stepMeta != null) {
                    try {
                        RowMetaInterface row = transMeta.getPrevStepFields(stepMeta);

                        // Remember these fields...
                        for (int i = 0; i < row.size(); i++) {
                            inputFields.put(row.getValueMeta(i).getName(), Integer.valueOf(i));
                        }

                        setComboBoxes();
                    } catch (KettleException e) {
                        log.logError(toString(), BaseMessages.getString(PKG, "MongoDbDeleteDialog.Log.UnableToFindInput"));
                    }
                }
            }
        };
        new Thread(runnable).start();

        // get fields but
        m_getFieldsBut = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
        props.setLook(m_getFieldsBut);
        m_getFieldsBut.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.GetFieldsBut")); //$NON-NLS-1$
        fd = new FormData();
        // fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, -margin * 2);
        fd.left = new FormAttachment(0, margin);
        m_getFieldsBut.setLayoutData(fd);

        m_getFieldsBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                getFields();
            }
        });

        m_previewDocStructBut = new Button(wFieldsComp, SWT.PUSH | SWT.CENTER);
        props.setLook(m_previewDocStructBut);
        m_previewDocStructBut.setText(BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewDocStructBut")); //$NON-NLS-1$
        fd = new FormData();
        // fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, -margin * 2);
        fd.left = new FormAttachment(m_getFieldsBut, margin);
        m_previewDocStructBut.setLayoutData(fd);
        m_previewDocStructBut.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                previewDocStruct();
            }
        });

        m_mongoFieldsView = new TableView(transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colInf, 1, lsMod, props);

        fd = new FormData();
        fd.top = new FormAttachment(0, margin * 2);
        fd.bottom = new FormAttachment(m_getFieldsBut, -margin * 2);
        fd.left = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        m_mongoFieldsView.setLayoutData(fd);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(0, 0);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, 0);
        wFieldsComp.setLayoutData(fd);

        wFieldsComp.layout();
        m_wMongoFieldsTab.setControl(wFieldsComp);

        fd = new FormData();
        fd.left = new FormAttachment(0, 0);
        fd.top = new FormAttachment(m_stepnameText, margin);
        fd.right = new FormAttachment(100, 0);
        fd.bottom = new FormAttachment(100, -50);
        m_wTabFolder.setLayoutData(fd);

        // Buttons inherited from BaseStepDialog
        wOK = new Button(shell, SWT.PUSH);
        wOK.setText(BaseMessages.getString(PKG, "System.Button.OK")); //$NON-NLS-1$

        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel")); //$NON-NLS-1$

        setButtonPositions(new Button[]{wOK, wCancel}, margin, m_wTabFolder);

        // Add listeners
        lsCancel = new Listener() {
            @Override
            public void handleEvent(Event e) {
                cancel();
            }
        };

        lsOK = new Listener() {
            @Override
            public void handleEvent(Event e) {
                ok();
            }
        };

        wCancel.addListener(SWT.Selection, lsCancel);
        wOK.addListener(SWT.Selection, lsOK);

        lsDef = new SelectionAdapter() {
            @Override
            public void widgetDefaultSelected(SelectionEvent e) {
                ok();
            }
        };

        m_stepnameText.addSelectionListener(lsDef);

        // Detect X or ALT-F4 or something that kills this window...
        shell.addShellListener(new ShellAdapter() {
            @Override
            public void shellClosed(ShellEvent e) {
                cancel();
            }
        });

        m_wTabFolder.setSelection(0);
        setSize();

        getData();

        shell.open();
        while (!shell.isDisposed()) {
            if (!display.readAndDispatch()) {
                display.sleep();
            }
        }

        return stepname;
    }

    protected void cancel() {
        stepname = null;
        m_currentMeta.setChanged(changed);

        dispose();
    }

    private void ok() {
        if (Const.isEmpty(m_stepnameText.getText())) {
            return;
        }

        stepname = m_stepnameText.getText();

        getInfo(m_currentMeta);

        if (m_currentMeta.getMongoFields() == null) {
            // popup dialog warning that no paths have been defined
            ShowMessageDialog smd =
                    new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.NoFieldPathsDefined.Title"), BaseMessages.getString(PKG, //$NON-NLS-1$
                    "MongoDbDeleteDialog.ErrorMessage.NoFieldPathsDefined")); //$NON-NLS-1$
            smd.open();
        }

        if (!m_originalMeta.equals(m_currentMeta)) {
            m_currentMeta.setChanged();
            changed = m_currentMeta.hasChanged();
        }

        dispose();
    }

    private void checkPasswordVisible() {
        String password = m_passField.getText();
        ArrayList<String> list = new ArrayList<String>();
        StringUtil.getUsedVariables(password, list, true);
        if (list.isEmpty()) {
            m_passField.setEchoChar('*');
        } else {
            m_passField.setEchoChar('\0'); // show everything
        }
    }

    private void setupDBNames() {
        String current = m_dbNameField.getText();
        m_dbNameField.removeAll();

        String hostname = transMeta.environmentSubstitute(m_hostnameField.getText());

        if (!Const.isEmpty(hostname)) {
            try {
                final MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
                getInfo(meta);
                List<String> dbNames = new ArrayList<String>();
                MongoClientWrapper wrapper = MongoClientWrapperFactory.createMongoClientWrapper(meta, transMeta, log);
                try {
                    dbNames = wrapper.getDatabaseNames();
                } finally {
                    wrapper.dispose();
                }
                for (String s : dbNames) {
                    m_dbNameField.add(s);
                }

            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e); //$NON-NLS-1$
                new ErrorDialog(shell, BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage." + "UnableToConnect"), //$NON-NLS-1$ //$NON-NLS-2$
                        BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e); //$NON-NLS-1$
            }
        } else {
            // popup some feedback
            ShowMessageDialog smd =
                    new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails.Title"), BaseMessages.getString(PKG, //$NON-NLS-1$
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails", "host name(s)")); //$NON-NLS-1$ //$NON-NLS-2$
            smd.open();
        }

        if (!Const.isEmpty(current)) {
            m_dbNameField.setText(current);
        }
    }

    private void setupCollectionNamesForDB(boolean quiet) {
        final String hostname = transMeta.environmentSubstitute(m_hostnameField.getText());
        final String dB = transMeta.environmentSubstitute(m_dbNameField.getText());

        String current = m_collectionField.getText();
        m_collectionField.removeAll();

        if (!Const.isEmpty(hostname) && !Const.isEmpty(dB)) {

            final MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
            getInfo(meta);
            try {
                MongoClientWrapper clientWrapper = MongoClientWrapperFactory.createMongoClientWrapper(meta, transMeta, log);
                Set<String> collections = new HashSet<String>();
                try {
                    collections = clientWrapper.getCollectionsNames(dB);
                } finally {
                    clientWrapper.dispose();
                }

                for (String c : collections) {
                    m_collectionField.add(c);
                }
            } catch (Exception e) {
                // Unwrap the PrivilegedActionException if it was thrown
                if (e instanceof PrivilegedActionException) {
                    e = ((PrivilegedActionException) e).getException();
                }
                logError(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e); //$NON-NLS-1$
                new ErrorDialog(shell, BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage." + "UnableToConnect"), //$NON-NLS-1$ //$NON-NLS-2$
                        BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e); //$NON-NLS-1$
            }
        } else {
            // popup some feedback

            String missingConnDetails = ""; //$NON-NLS-1$
            if (Const.isEmpty(hostname)) {
                missingConnDetails += "host name(s)"; //$NON-NLS-1$
            }
            if (Const.isEmpty(dB)) {
                missingConnDetails += " database"; //$NON-NLS-1$
            }
            ShowMessageDialog smd =
                    new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails.Title"), BaseMessages.getString(PKG, //$NON-NLS-1$
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails", missingConnDetails)); //$NON-NLS-1$
            smd.open();
        }

        if (!Const.isEmpty(current)) {
            m_collectionField.setText(current);
        }
    }

    private void setupCustomWriteConcernNames() {
        String hostname = transMeta.environmentSubstitute(m_hostnameField.getText());

        if (!Const.isEmpty(hostname)) {
            MongoDbDeleteMeta meta = new MongoDbDeleteMeta();
            getInfo(meta);
            try {
                MongoClientWrapper wrapper = MongoClientWrapperFactory.createMongoClientWrapper(meta, transMeta, log);
                List<String> custom = new ArrayList<String>();
                try {
                    custom = wrapper.getLastErrorModes();
                } finally {
                    wrapper.dispose();
                }

                if (custom.size() > 0) {
                    String current = m_writeConcern.getText();
                    m_writeConcern.removeAll();

                    for (String s : custom) {
                        m_writeConcern.add(s);
                    }

                    if (!Const.isEmpty(current)) {
                        m_writeConcern.setText(current);
                    }
                }
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e);
                new ErrorDialog(shell, BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage." + "UnableToConnect"),
                        BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.UnableToConnect"), e);
            }
        } else {
            ShowMessageDialog smd =
                    new ShowMessageDialog(shell, SWT.ICON_WARNING | SWT.OK, BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails.Title"), BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.MissingConnectionDetails", "host name(s)"));
            smd.open();
        }
    }

    private void getFields() {
        try {
            RowMetaInterface r = transMeta.getPrevStepFields(stepname);
            if (r != null) {
                BaseStepDialog.getFieldsFromPrevious(r, m_mongoFieldsView, 1, new int[]{1, 3}, null, -1, -1, null);
            }
        } catch (KettleException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
                    e);
            new ErrorDialog(shell,
                    BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"), BaseMessages.getString(PKG,
                    "System.Dialog.GetFieldsFailed.Message"), e);
        }
    }

    private void getInfo(MongoDbDeleteMeta meta) {
        meta.setHostnames(m_hostnameField.getText());
        meta.setPort(m_portField.getText());
        meta.setUseAllReplicaSetMembers(m_useAllReplicaSetMembersBut.getSelection());
        meta.setAuthenticationUser(m_usernameField.getText());
        meta.setAuthenticationPassword(m_passField.getText());
        meta.setUseKerberosAuthentication(m_kerberosBut.getSelection());
        meta.setDbName(m_dbNameField.getText());
        meta.setCollection(m_collectionField.getText());
        meta.setConnectTimeout(m_connectTimeout.getText());
        meta.setSocketTimeout(m_socketTimeout.getText());
        meta.setWriteConcern(m_writeConcern.getText());
        meta.setWTimeout(m_wTimeout.getText());
        meta.setJournal(m_journalWritesCheck.getSelection());
        meta.setReadPreference(m_readPreference.getText());
        meta.setWriteRetries(m_writeRetries.getText());
        meta.setWriteRetryDelay(m_writeRetryDelay.getText());

        meta.setMongoFields(tableToMongoFieldList());

    }

    private List<MongoDbDeleteMeta.MongoField> tableToMongoFieldList() {
        int numNonEmpty = m_mongoFieldsView.nrNonEmpty();
        if (numNonEmpty > 0) {
            List<MongoDbDeleteMeta.MongoField> mongoFields = new ArrayList<MongoDbDeleteMeta.MongoField>();

            for (int i = 0; i < numNonEmpty; i++) {
                TableItem item = m_mongoFieldsView.getNonEmpty(i);
                String path = item.getText(1).trim();
                String comparator = item.getText(2).trim();
                String field1 = item.getText(3).trim();
                String field2 = item.getText(4).trim();

                MongoDbDeleteMeta.MongoField newField = new MongoDbDeleteMeta.MongoField();
                newField.m_mongoDocPath = path;
                if (Const.isEmpty(comparator)) {
                    comparator = Comparator.EQUAL.getValue();
                }
                newField.m_comparator = comparator;
                newField.m_incomingField1 = field1;
                newField.m_incomingField2 = field2;
                mongoFields.add(newField);
            }

            return mongoFields;
        }

        return null;
    }

    private void getData() {
        m_hostnameField.setText(Const.NVL(m_currentMeta.getHostnames(), "")); //$NON-NLS-1$
        m_portField.setText(Const.NVL(m_currentMeta.getPort(), "")); //$NON-NLS-1$
        m_useAllReplicaSetMembersBut.setSelection(m_currentMeta.getUseAllReplicaSetMembers());
        m_usernameField.setText(Const.NVL(m_currentMeta.getAuthenticationUser(), "")); //$NON-NLS-1$
        m_passField.setText(Const.NVL(m_currentMeta.getAuthenticationPassword(), "")); //$NON-NLS-1$
        m_kerberosBut.setSelection(m_currentMeta.getUseKerberosAuthentication());
        m_passField.setEnabled(!m_kerberosBut.getSelection());
        m_dbNameField.setText(Const.NVL(m_currentMeta.getDbName(), "")); //$NON-NLS-1$
        m_collectionField.setText(Const.NVL(m_currentMeta.getCollection(), "")); //$NON-NLS-1$

        m_connectTimeout.setText(Const.NVL(m_currentMeta.getConnectTimeout(), "")); //$NON-NLS-1$
        m_socketTimeout.setText(Const.NVL(m_currentMeta.getSocketTimeout(), "")); //$NON-NLS-1$
        m_writeConcern.setText(Const.NVL(m_currentMeta.getWriteConcern(), "")); //$NON-NLS-1$
        m_wTimeout.setText(Const.NVL(m_currentMeta.getWTimeout(), "")); //$NON-NLS-1$
        m_journalWritesCheck.setSelection(m_currentMeta.getJournal());
        m_readPreference.setText(Const.NVL(m_currentMeta.getReadPreference(), "")); //$NON-NLS-1$
        m_writeRetries.setText(Const.NVL(m_currentMeta.getWriteRetries(), "" //$NON-NLS-1$
                + MongoDbDeleteMeta.RETRIES));
        m_writeRetryDelay.setText(Const.NVL(m_currentMeta.getWriteRetryDelay(), "" //$NON-NLS-1$
                + MongoDbDeleteMeta.RETRIES));

        List<MongoDbDeleteMeta.MongoField> mongoFields = m_currentMeta.getMongoFields();

        if (mongoFields != null && mongoFields.size() > 0) {
            for (MongoDbDeleteMeta.MongoField field : mongoFields) {
                TableItem item = new TableItem(m_mongoFieldsView.table, SWT.NONE);

                item.setText(1, Const.NVL(field.m_mongoDocPath, ""));
                item.setText(2, Const.NVL(field.m_comparator, ""));
                item.setText(3, Const.NVL(field.m_incomingField1, ""));
                item.setText(4, Const.NVL(field.m_incomingField2, ""));
            }

            m_mongoFieldsView.removeEmptyRows();
            m_mongoFieldsView.setRowNums();
            m_mongoFieldsView.optWidth(true);
        }
    }

    protected void setComboBoxes() {
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        fields.putAll(inputFields);

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>(keySet);

        String fieldNames[] = entries.toArray(new String[entries.size()]);

        Const.sortStrings(fieldNames);
        colInf[2].setComboValues(fieldNames);
        colInf[2].setReadOnly(false);
        colInf[3].setComboValues(fieldNames);
        colInf[3].setReadOnly(false);
    }

    private void previewDocStruct() {
        List<MongoDbDeleteMeta.MongoField> mongoFields = tableToMongoFieldList();

        if (mongoFields == null || mongoFields.isEmpty()) {
            return;
        }

        // Try and get meta data on incoming fields
        RowMetaInterface actualR = null;
        RowMetaInterface r;
        boolean gotGenuineRowMeta = false;
        try {
            actualR = transMeta.getPrevStepFields(stepname);
            gotGenuineRowMeta = true;
        } catch (KettleException e) {
            // don't complain if we can't
        }
        r = new RowMeta();

        Object[] dummyRow = new Object[mongoFields.size() * 2]; // multiply by 2, because possiblity use between that required 2 value
        int i = 0;
        try {
            for (MongoDbDeleteMeta.MongoField field : mongoFields) {
                // set up dummy row meta
                if (!Const.isEmpty(field.m_incomingField1) && !Const.isEmpty(field.m_incomingField2)) {
                    ValueMetaInterface vm1 = ValueMetaFactory.createValueMeta(ValueMetaInterface.TYPE_STRING);
                    vm1.setName(field.m_incomingField1);
                    r.addValueMeta(vm1);

                    ValueMetaInterface vm2 = ValueMetaFactory.createValueMeta(ValueMetaInterface.TYPE_STRING);
                    vm2.setName(field.m_incomingField2);
                    r.addValueMeta(vm2);

                    String val1 = getValueToDisplay(gotGenuineRowMeta, actualR, field.m_incomingField1);
                    dummyRow[i++] = val1;

                    String val2 = getValueToDisplay(gotGenuineRowMeta, actualR, field.m_incomingField2);
                    dummyRow[i++] = val2;

                } else {
                    ValueMetaInterface vm = ValueMetaFactory.createValueMeta(ValueMetaInterface.TYPE_STRING);
                    vm.setName(field.m_incomingField1);
                    r.addValueMeta(vm);
                    String val = getValueToDisplay(gotGenuineRowMeta, actualR, field.m_incomingField1);
                    dummyRow[i++] = val;
                }
            }

            VariableSpace vs = new Variables();
            for (MongoDbDeleteMeta.MongoField m : mongoFields) {
                m.init(vs);
            }

            String toDisplay = "";
            String windowTitle = BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewDocStructure.Title");
            DBObject query = MongoDbDeleteData.getQueryObject(mongoFields, r, dummyRow, vs);
            toDisplay = BaseMessages.getString(PKG, "MongoDbDeleteDialog.PreviewModifierUpdate.Heading1")
                    + ": \n\n"
                    + prettyPrintDocStructure(query.toString());

            ShowMessageDialog smd =
                    new ShowMessageDialog(shell, SWT.ICON_INFORMATION | SWT.OK, windowTitle, toDisplay, true);
            smd.open();
        } catch (Exception ex) {
            logError(BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
                    + ":\n\n" + ex.getMessage(), ex);
            new ErrorDialog(shell, BaseMessages.getString(PKG,
                    "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Title"),
                    BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.ProblemPreviewingDocStructure.Message")
                    + ":\n\n" + ex.getMessage(), ex);
            return;
        }
    }

    private String getValueToDisplay(boolean genuineRowMeta, RowMetaInterface rmi, String fieldName) {
        String val = "";
        if (genuineRowMeta && rmi.indexOfValue(fieldName) >= 0) {
            int index = rmi.indexOfValue(fieldName);
            switch (rmi.getValueMeta(index).getType()) {
                case ValueMetaInterface.TYPE_STRING:
                    val = "<string val>";
                    break;
                case ValueMetaInterface.TYPE_INTEGER:
                    val = "<integer val>";
                    break;
                case ValueMetaInterface.TYPE_NUMBER:
                    val = "<number val>";
                    break;
                case ValueMetaInterface.TYPE_BOOLEAN:
                    val = "<bool val>";
                    break;
                case ValueMetaInterface.TYPE_DATE:
                    val = "<date val>";
                    break;
                case ValueMetaInterface.TYPE_BINARY:
                    val = "<binary val>";
                    break;
                default:
                    val = "<unsupported value type>";
            }
        } else {
            val = "<value>";
        }
        return val;
    }

    private static enum Element {

        OPEN_BRACE, CLOSE_BRACE, OPEN_BRACKET, CLOSE_BRACKET, COMMA
    };

    private static void pad(StringBuffer toPad, int numBlanks) {
        for (int i = 0; i < numBlanks; i++) {
            toPad.append(' ');
        }
    }

    public static String prettyPrintDocStructure(String toFormat) {
        StringBuffer result = new StringBuffer();
        int indent = 0;
        String source = toFormat.replaceAll("[ ]*,", ","); //$NON-NLS-1$ //$NON-NLS-2$
        Element next = Element.OPEN_BRACE;

        while (source.length() > 0) {
            source = source.trim();
            String toIndent = ""; //$NON-NLS-1$
            int minIndex = Integer.MAX_VALUE;
            char targetChar = '{';
            if (source.indexOf('{') > -1 && source.indexOf('{') < minIndex) {
                next = Element.OPEN_BRACE;
                minIndex = source.indexOf('{');
                targetChar = '{';
            }
            if (source.indexOf('}') > -1 && source.indexOf('}') < minIndex) {
                next = Element.CLOSE_BRACE;
                minIndex = source.indexOf('}');
                targetChar = '}';
            }
            if (source.indexOf('[') > -1 && source.indexOf('[') < minIndex) {
                next = Element.OPEN_BRACKET;
                minIndex = source.indexOf('[');
                targetChar = '[';
            }
            if (source.indexOf(']') > -1 && source.indexOf(']') < minIndex) {
                next = Element.CLOSE_BRACKET;
                minIndex = source.indexOf(']');
                targetChar = ']';
            }
            if (source.indexOf(',') > -1 && source.indexOf(',') < minIndex) {
                next = Element.COMMA;
                minIndex = source.indexOf(',');
                targetChar = ',';
            }

            if (minIndex == 0) {
                if (next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET) {
                    indent -= 2;
                }
                pad(result, indent);
                String comma = ""; //$NON-NLS-1$
                int offset = 1;
                if (source.length() >= 2 && source.charAt(1) == ',') {
                    comma = ","; //$NON-NLS-1$
                    offset = 2;
                }
                result.append(targetChar).append(comma).append("\n"); //$NON-NLS-1$
                source = source.substring(offset, source.length());
            } else {
                pad(result, indent);
                if (next == Element.CLOSE_BRACE || next == Element.CLOSE_BRACKET) {
                    toIndent = source.substring(0, minIndex);
                    source = source.substring(minIndex, source.length());
                } else {
                    toIndent = source.substring(0, minIndex + 1);
                    source = source.substring(minIndex + 1, source.length());
                }
                result.append(toIndent.trim()).append("\n"); //$NON-NLS-1$
            }

            if (next == Element.OPEN_BRACE || next == Element.OPEN_BRACKET) {
                indent += 2;
            }
        }

        return result.toString();
    }
}
