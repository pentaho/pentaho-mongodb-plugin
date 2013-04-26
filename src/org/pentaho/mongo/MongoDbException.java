package org.pentaho.mongo;

import org.pentaho.di.core.exception.KettleException;

public class MongoDbException extends KettleException {

  public MongoDbException() {
    super();
  }

  public MongoDbException(String message, Throwable cause) {
    super(message, cause);
  }

  public MongoDbException(String message) {
    super(message);
  }

  public MongoDbException(Throwable cause) {
    super(cause);
  }

  private static final long serialVersionUID = -5312035742249234075L;

}
