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
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.mongo.AuthContext;
import org.pentaho.mongo.wrapper.collection.MongoCollectionWrapper;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoKerberosWrapperTest {
  @SuppressWarnings( "unchecked" )
  @Test
  public void testWrapProperlyWrapsCollection() throws KettleException, PrivilegedActionException {
    MongoClient client = mock( MongoClient.class );
    AuthContext authContext = mock( AuthContext.class );
    LogChannelInterface log = mock( LogChannelInterface.class );
    final DBCollection dbCollection = mock( DBCollection.class );
    String username = "test";
    final KerberosMongoClientWrapper wrapper = new KerberosMongoClientWrapper( client, log, username, authContext );
    MongoCollectionWrapper mongoCollectionWrapper = wrapper.wrap( dbCollection );
    when( authContext.doAs( any( PrivilegedExceptionAction.class ) ) ).thenAnswer( new Answer<Void>() {

      @Override
      public Void answer( InvocationOnMock invocation ) throws Throwable {
        dbCollection.drop();
        return null;
      }
    } );
    mongoCollectionWrapper.drop();
    verify( authContext, times( 1 ) ).doAs( any( PrivilegedExceptionAction.class ) );
    verify( dbCollection, times( 1 ) ).drop();
  }
}
