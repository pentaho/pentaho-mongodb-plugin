package org.pentaho.mongo.wrapper.cursor;

import org.pentaho.di.core.exception.KettleException;

import com.mongodb.DBObject;
import com.mongodb.ServerAddress;

public interface MongoCursorWrapper {

  boolean hasNext() throws KettleException;

  DBObject next() throws KettleException;

  ServerAddress getServerAddress() throws KettleException;

  void close() throws KettleException;

  MongoCursorWrapper limit( int i ) throws KettleException;

}
