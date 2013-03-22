package org.pentaho.di.ui.swing.preview;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.ui.xul.common.preview.AbstractPreviewRowsXulDialog;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.XulSettingsManager;
import org.pentaho.ui.xul.components.XulTreeCol;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.containers.XulTreeCols;
import org.pentaho.ui.xul.containers.XulTreeRow;
import org.pentaho.ui.xul.swing.SwingBindingFactory;
import org.pentaho.ui.xul.swing.SwingXulLoader;
import org.pentaho.ui.xul.swing.SwingXulRunner;
import org.pentaho.ui.xul.swing.tags.SwingTreeCell;
import org.pentaho.ui.xul.swing.tags.SwingTreeCol;
import org.pentaho.ui.xul.swing.tags.SwingTreeCols;

public class PreviewRowsSwingDialog extends AbstractPreviewRowsXulDialog {

  @Override
  public void init() {
    super.init();
  }

  public PreviewRowsSwingDialog(Object parent, BaseStepMeta meta, int previewRowCount) {
    super(parent, meta, previewRowCount);
  }

  @Override
  public XulSettingsManager getSettingsManager() {
    return null;
  }

  @Override
  public ResourceBundle getResourceBundle() {
    return null;
  }

  protected void initializeXul() throws XulException {

    initializeXul(new SwingXulLoader(), new SwingBindingFactory(), new SwingXulRunner(), parent);
      
  }

  @Override
  public void onAccept() {
    close();
    dispose();

  }

  @Override
  public void onCancel() {
    close();
    dispose();

  }

  @Override
  protected Class<?> getClassForMessages() {
    return this.getClass();
  }

  @Override
  public void dispose() {

  }
  
  /**
   * TODO: replace this method with XUL bindings 
   * 
   * This is a bad bad method. We need a way to load the column definitions and 
   * data through standard XUL bindings.
   * 
   * @param data
   * @param columns
   */
  protected void createPreviewRows(List<Object[]> data, List<String> columns) {

    // Adds table rows.
    Object[] theObj = null;
    XulTreeRow theRow = null;
    Object theValue = null;
    SwingTreeCell theCell = null;

    XulTree table = (XulTree) super.document.getElementById("tableData");
    table.getRootChildren().removeAll();
    Iterator<Object[]> theItr = data.iterator();
    while (theItr.hasNext()) {
      theObj = theItr.next();
      theRow = table.getRootChildren().addNewRow();
      for (int i = 0; i < theObj.length; i++) {
        theValue = theObj[i];
        theCell = new SwingTreeCell(null);
        theCell.setLabel(theValue == null ? "" : theValue.toString());          
        theRow.addCell(theCell);
      }
    }

    // Adds table columns.
    SwingTreeCol theColumn = null;

    SwingTreeCols theColumns = new SwingTreeCols(null, table, null, null);
    for (int i = 0; i < columns.size(); i++) {
      theColumn = new SwingTreeCol(null, null, null, null);
      theColumn.setWidth(100);
      theColumn.setLabel(columns.get(i));
      theColumns.addColumn(theColumn);
    }
    table.setColumns(theColumns);
    table.update();

  }

}
