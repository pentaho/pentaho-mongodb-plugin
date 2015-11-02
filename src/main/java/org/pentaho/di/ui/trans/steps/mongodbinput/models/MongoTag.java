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
