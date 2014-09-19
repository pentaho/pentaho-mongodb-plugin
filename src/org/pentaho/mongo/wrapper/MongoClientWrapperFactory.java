package org.pentaho.mongo.wrapper;

import java.lang.reflect.Proxy;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;

public class MongoClientWrapperFactory {
  public static MongoClientWrapper createMongoClientWrapper( MongoDbMeta meta, VariableSpace vars, LogChannelInterface log )
    throws KettleException {
    if ( meta.getUseKerberosAuthentication() ) {
      KerberosMongoClientWrapper wrapper = new KerberosMongoClientWrapper( meta, vars, log );
      return (MongoClientWrapper) Proxy.newProxyInstance( wrapper.getClass().getClassLoader(),
          new Class<?>[] { MongoClientWrapper.class }, new KerberosInvocationHandler( wrapper.getAuthContext(), wrapper ) );
    } else {
      String user = vars.environmentSubstitute(meta.getAuthenticationUser());
      String password = vars.environmentSubstitute(meta.getAuthenticationPassword());
      if (!Const.isEmpty(user) || !Const.isEmpty(password)) {
        return new UsernamePasswordMongoClientWrapper(meta, vars, log);
      } else {
        return new NoAuthMongoClientWrapper(meta, vars, log);
      }
    }
  }
}
