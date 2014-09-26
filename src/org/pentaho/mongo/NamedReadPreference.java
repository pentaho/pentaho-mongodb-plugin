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
package org.pentaho.mongo;
import java.util.ArrayList;
import java.util.Collection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;
public enum NamedReadPreference {
  PRIMARY( ReadPreference.primary() ),
  PRIMARY_PREFERRED( ReadPreference.primaryPreferred() ),
  SECONDARY( ReadPreference.secondary() ),
  SECONDARY_PREFERRED( ReadPreference.secondaryPreferred() ),
  NEAREST( ReadPreference.nearest() );
  private ReadPreference pref = null;
  NamedReadPreference( ReadPreference pref ) {
    this.pref = pref;
  }
  public String getName() {
    return pref.getName();
  }
  public ReadPreference getPreference() {
    return pref;
  }
  public static Collection<String> getPreferenceNames() {
    ArrayList<String> prefs = new ArrayList<String>();
    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      prefs.add( preference.getName() );
    }
    return prefs;
  }
  public ReadPreference getTaggableReadPreference( DBObject firstTagSet, DBObject... remainingTagSets ) {
    switch ( this ) {
      case PRIMARY_PREFERRED:
        return ReadPreference.primaryPreferred( firstTagSet, remainingTagSets );
      case SECONDARY:
        return ReadPreference.secondary( firstTagSet, remainingTagSets );
      case SECONDARY_PREFERRED:
        return ReadPreference.secondaryPreferred( firstTagSet, remainingTagSets );
      case NEAREST:
        return ReadPreference.nearest( firstTagSet, remainingTagSets );
      default:
        return ( pref instanceof TaggableReadPreference ) ? pref : null;
    }
  }
  public static NamedReadPreference byName( String preferenceName ) {
    NamedReadPreference foundPreference = null;
    for ( NamedReadPreference preference : NamedReadPreference.values() ) {
      if ( preference.getName().equalsIgnoreCase( preferenceName ) ) {
        foundPreference = preference;
        break;
      }
    }
    return foundPreference;
  }
}
