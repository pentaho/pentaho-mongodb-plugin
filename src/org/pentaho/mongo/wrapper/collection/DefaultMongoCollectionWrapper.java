package org.pentaho.mongo.wrapper.collection;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.wrapper.cursor.DefaultCursorWrapper;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class DefaultMongoCollectionWrapper implements MongoCollectionWrapper {
  private final DBCollection collection;

  public DefaultMongoCollectionWrapper( DBCollection collection ) {
    this.collection = collection;
  }

  @Override
  public MongoCursorWrapper find( DBObject dbObject, DBObject dbObject2 ) throws KettleException {
    return wrap( collection.find( dbObject, dbObject2 ) );
  }

  @Override
  public AggregationOutput aggregate( DBObject firstP, DBObject[] remainder ) throws KettleException {
    return collection.aggregate( firstP, remainder );
  }

  @Override
  public MongoCursorWrapper find() throws KettleException {
    return wrap( collection.find() );
  }

  @Override
  public void drop() throws KettleException {
    collection.drop();
  }

  @Override
  public WriteResult update( DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi )
    throws KettleException {
    return collection.update( updateQuery, insertUpdate, upsert, multi );
  }

  @Override
  public WriteResult insert( List<DBObject> m_batch ) throws KettleException {
    return collection.insert( m_batch );
  }

  @Override
  public MongoCursorWrapper find( DBObject query ) throws KettleException {
    return wrap( collection.find( query ) );
  }

  @Override
  public void dropIndex( BasicDBObject mongoIndex ) throws KettleException {
    collection.dropIndex( mongoIndex );
  }

  @Override
  public void createIndex( BasicDBObject mongoIndex ) throws KettleException {
    collection.createIndex( mongoIndex );
  }

  @Override
  public WriteResult save( DBObject toTry ) throws KettleException {
    return collection.save( toTry );
  }

  protected MongoCursorWrapper wrap( DBCursor cursor ) {
    return new DefaultCursorWrapper( cursor );
  }
}
