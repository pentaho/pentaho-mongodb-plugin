package org.pentaho.mongo;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

/**
 * A context for executing authorized actions on behalf of an authenticated user
 * via a {@link LoginContext}.
 * 
 * @author Jordan Ganoff <jganoff@pentaho.com>
 * 
 */
public class AuthContext {
  private LoginContext login;

  /**
   * Create a context for the given login. If the login is null all operations
   * will be done as the current user.
   * 
   * TODO Prevent null login contexts and create login contexts for the current
   * OS user instead. This will keep the implementation cleaner.
   * 
   * @param login
   */
  public AuthContext(LoginContext login) {
    this.login = login;
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no
   * user is explicitly authenticated the action will be executed as the current
   * user.
   * 
   * @param action
   *          The action to execute
   * @return The return value of the action
   */
  public <T> T doAs(PrivilegedAction<T> action) {
    if (login == null) {
      // If a user is not explicitly authenticated directly execute the action
      return action.run();
    } else {
      return Subject.doAs(login.getSubject(), action);
    }
  }

  /**
   * Execute an action on behalf of the login used to create this context. If no
   * user is explicitly authenticated the action will be executed as the current
   * user.
   * 
   * @param action
   *          The action to execute
   * @return The return value of the action
   * @throws PrivilegedActionException
   *           If an exception occurs while executing the action. The cause of
   *           the exception will be provided in
   *           {@link PrivilegedActionException#getCause()}.
   */
  public <T> T doAs(PrivilegedExceptionAction<T> action) throws PrivilegedActionException {
    if (login == null) {
      // If a user is not explicitly authenticated directly execute the action
      try {
        return action.run();
      } catch (Exception ex) {
        // Wrap any exceptions throw in a PrivilegedActionException just as
        // would be thrown when executed via Subject.doAs(..)
        throw new PrivilegedActionException(ex);
      }
    } else {
      return Subject.doAs(login.getSubject(), action);
    }
  }
}
