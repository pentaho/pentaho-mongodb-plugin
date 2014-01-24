package org.pentaho.mongo.wrapper.collection;

import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.mongo.wrapper.cursor.MongoCursorWrapper;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public interface MongoCollectionWrapper {

  MongoCursorWrapper find( DBObject dbObject, DBObject dbObject2 ) throws KettleException;

  AggregationOutput aggregate( DBObject firstP, DBObject[] remainder ) throws KettleException;

  MongoCursorWrapper find() throws KettleException;

  void drop() throws KettleException;

  WriteResult update( DBObject updateQuery, DBObject insertUpdate, boolean upsert, boolean multi )
    throws KettleException;

  WriteResult insert( List<DBObject> m_batch ) throws KettleException;

  MongoCursorWrapper find( DBObject query ) throws KettleException;

  void dropIndex( BasicDBObject mongoIndex ) throws KettleException;

  void createIndex( BasicDBObject mongoIndex ) throws KettleException;

  WriteResult save( DBObject toTry ) throws KettleException;
}
