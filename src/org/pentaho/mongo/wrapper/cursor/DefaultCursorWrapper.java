package org.pentaho.mongo.wrapper.cursor;

import org.pentaho.di.core.exception.KettleException;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;

public class DefaultCursorWrapper implements MongoCursorWrapper {
  private final DBCursor cursor;

  public DefaultCursorWrapper( DBCursor cursor ) {
    this.cursor = cursor;
  }

  @Override
  public boolean hasNext() throws KettleException {
    return cursor.hasNext();
  }

  @Override
  public DBObject next() throws KettleException {
    return cursor.next();
  }

  @Override
  public ServerAddress getServerAddress() throws KettleException {
    return cursor.getServerAddress();
  }

  @Override
  public void close() throws KettleException {
    cursor.close();
  }

  @Override
  public MongoCursorWrapper limit( int i ) throws KettleException {
    return wrap( cursor.limit( i ) );
  }

  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new DefaultCursorWrapper( cursor );
  }
}
