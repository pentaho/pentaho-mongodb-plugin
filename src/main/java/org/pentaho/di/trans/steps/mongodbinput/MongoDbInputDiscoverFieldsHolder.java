package org.pentaho.di.trans.steps.mongodbinput;

/**
 * Created by bryan on 11/10/14.
 */
public class MongoDbInputDiscoverFieldsHolder {
    private static MongoDbInputDiscoverFieldsHolder INSTANCE;
    private MongoDbInputDiscoverFields mongoDbInputDiscoverFields;

    public MongoDbInputDiscoverFieldsHolder( MongoDbInputDiscoverFields mongoDbInputDiscoverFields ) {
        synchronized ( MongoDbInputDiscoverFieldsHolder.class ) {
            if ( INSTANCE != null ) {
                throw new IllegalStateException( "This object should only be constructed by the blueprint" );
            }
            this.mongoDbInputDiscoverFields = mongoDbInputDiscoverFields;
            INSTANCE = this;
        }
    }

    public static MongoDbInputDiscoverFieldsHolder getInstance() {
        return INSTANCE;
    }

    public MongoDbInputDiscoverFields getMongoDbInputDiscoverFields() {
        return mongoDbInputDiscoverFields;
    }
}
