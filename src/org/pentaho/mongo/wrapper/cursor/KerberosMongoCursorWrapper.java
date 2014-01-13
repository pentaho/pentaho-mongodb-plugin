package org.pentaho.mongo.wrapper.cursor;

import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.KerberosInvocationHandler;

import com.mongodb.DBCursor;

public class KerberosMongoCursorWrapper extends DefaultCursorWrapper {
  private final AuthContext authContext;

  public KerberosMongoCursorWrapper( DBCursor cursor, AuthContext authContext ) {
    super( cursor );
    this.authContext = authContext;
  }

  @Override
  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return KerberosInvocationHandler.wrap( MongoCursorWrapper.class, authContext, new KerberosMongoCursorWrapper(
        cursor, authContext ) );
  }

}
