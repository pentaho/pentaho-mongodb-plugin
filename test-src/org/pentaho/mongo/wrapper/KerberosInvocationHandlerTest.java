package org.pentaho.mongo.wrapper;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.AuthContext;

public class KerberosInvocationHandlerTest {
  @SuppressWarnings( "unchecked" )
  @Test
  public void testInvocationHandlerCallsDoAsWhichCallsDelegate() throws KettleException, PrivilegedActionException {
    final MongoClientWrapper wrapper = mock( MongoClientWrapper.class );
    AuthContext authContext = mock( AuthContext.class );
    MongoClientWrapper wrappedWrapper = KerberosInvocationHandler.wrap( MongoClientWrapper.class, authContext, wrapper );
    when( authContext.doAs( any( PrivilegedExceptionAction.class ) ) ).thenAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        wrapper.dispose();
        return null;
      }
    } );
    wrappedWrapper.dispose();
    verify( authContext, times( 1 ) ).doAs( any( PrivilegedExceptionAction.class ) );
    verify( wrapper, times( 1 ) ).dispose();
  }
}
