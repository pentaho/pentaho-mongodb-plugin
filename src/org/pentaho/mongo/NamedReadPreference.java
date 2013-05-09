package org.pentaho.mongo;

import java.util.ArrayList;
import java.util.Collection;

import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.TaggableReadPreference;

public enum NamedReadPreference {
  
  PRIMARY(ReadPreference.primary()),
  PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
  SECONDARY(ReadPreference.secondary()),
  SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
  NEAREST(ReadPreference.nearest());
  
  private ReadPreference pref = null;
  
  NamedReadPreference(ReadPreference pref){
    this.pref = pref;
  }
  
  public String getName(){
    return pref.getName();
  }
  
  public ReadPreference getPreference(){
    return pref;
  }
  
  public static Collection<String> getPreferenceNames(){
    ArrayList<String> prefs = new ArrayList<String>();

    for (NamedReadPreference preference: NamedReadPreference.values()){
      prefs.add(preference.getName());
    }
    
    return prefs;
  }
  
  public ReadPreference getTaggableReadPreference(DBObject firstTagSet, DBObject... remainingTagSets){
    
    switch (this){
      case PRIMARY_PREFERRED:
        return ReadPreference.primaryPreferred(firstTagSet, remainingTagSets);
      case SECONDARY:
        return ReadPreference.secondary(firstTagSet, remainingTagSets);
      case SECONDARY_PREFERRED:
        return ReadPreference.secondaryPreferred(firstTagSet, remainingTagSets);
      case NEAREST: 
        return ReadPreference.nearest(firstTagSet, remainingTagSets);
      default:
        return (pref instanceof TaggableReadPreference) ? pref : null;
    }
  }
  
  public static NamedReadPreference byName (String preferenceName){
    NamedReadPreference foundPreference = null;
    
    for (NamedReadPreference preference: NamedReadPreference.values()){
      if (preference.getName().equalsIgnoreCase(preferenceName)){
        foundPreference = preference;
        break;
      }
    }
    return foundPreference;
  }

}
