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


package org.pentaho.di.ui.trans.steps.mongodbinput.models;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.ui.xul.XulEventSourceAdapter;
import org.pentaho.ui.xul.util.AbstractModelList;

public class MongoTag extends XulEventSourceAdapter {

  private String m_tagName = "";

  public MongoTag() {

  }

  public MongoTag( String name ) {
    m_tagName = name;
  }

  public String getTagName() {
    return this.m_tagName;
  }

  public void setTagName( String name ) {
    this.m_tagName = name;
  }

  public static void convertList( List<String> tags, AbstractModelList<MongoTag> docTags ) {

    if ( tags == null || tags.isEmpty() ) {
      return;
    }
    for ( String tag : tags ) {
      MongoTag docTag = new MongoTag();

      if ( tag.startsWith( "{" ) ) {
        tag = tag.substring( 1 );
      }

      if ( tag.endsWith( "}" ) ) {
        tag = tag.substring( 0, tag.length() - 1 );
      }

      docTag.setTagName( tag );
      docTags.add( docTag );
    }
  }

  public static void trimList( List<String> tags, AbstractModelList<MongoTag> docTags ) {

    if ( tags == null || tags.isEmpty() ) {
      return;
    }

    for ( int i = tags.size() - 1; i >= 0; i-- ) {
      String tag = tags.get( i );
      for ( MongoTag docTag : docTags.asList() ) {
        if ( tag.equalsIgnoreCase( docTag.getTagName() ) ) {
          tags.remove( tag );
          break;
        }
      }
    }
  }

  public static List<String> convertFromList( AbstractModelList<MongoTag> docTags ) {
    List<String> tags = new ArrayList<String>();

    if ( docTags == null || docTags.isEmpty() ) {
      return tags;
    }
    for ( MongoTag docTag : docTags ) {

      String tag = docTag.getTagName();

      if ( !tag.startsWith( "{" ) ) {
        tag = "{" + tag;
      }

      if ( !tag.endsWith( "}" ) ) {
        tag += "}";
      }

      tags.add( tag );
    }
    return tags;
  }
}
