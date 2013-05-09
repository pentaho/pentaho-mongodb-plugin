package org.pentaho.di.ui.trans.steps.mongodbinput.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData.MongoField;
import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.util.AbstractModelList;

public class MongoTag extends XulEventSourceAdapter{

  private String m_tagName = "";
  
  public MongoTag(){
    
  }
  
  public MongoTag(String name){
    m_tagName = name;
  }
  
  public String getTagName(){
    return this.m_tagName;
  }
    
  public void setTagName(String name){
    this.m_tagName = name;
  }
  
  
  public static void convertList(List<String> tags, AbstractModelList<MongoTag> docTags){
    
    if ( tags == null || tags.isEmpty()){
      return;
    }
    for (String tag: tags){
      MongoTag docTag = new MongoTag();

      if (tag.startsWith("{")) { 
        tag = tag.substring(1); 
      }
      
      if (tag.endsWith("}")) { 
        tag = tag.substring(0, tag.length() - 1);
      }

      docTag.setTagName(tag);
      docTags.add(docTag);
    }
  }

  public static List<String> convertFromList(AbstractModelList<MongoTag> docTags){
    List<String> tags = new ArrayList<String>();
    
    if ( docTags == null || docTags.isEmpty()){
      return tags;
    }
    for (MongoTag docTag: docTags){

      String tag = docTag.getTagName();
      
      if (!tag.startsWith("{")) { 
        tag = "{" + tag; 
      }
      
      if (!tag.endsWith("}")) { 
        tag += "}"; 
      }

      tags.add(tag);
    }
    return tags;
  }
}
