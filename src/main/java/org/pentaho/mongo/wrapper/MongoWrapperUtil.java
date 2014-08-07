package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.MongoProp;
import org.pentaho.mongo.MongoProperties;

/**
 * Created by bryan on 8/7/14.
 */
public class MongoWrapperUtil {
    public static MongoClientWrapper createMongoClientWrapper(MongoDbMeta mongoDbMeta, VariableSpace vars, LogChannelInterface log) throws MongoDbException {
        MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();
        propertiesBuilder.set(MongoProp.HOST, vars.environmentSubstitute(mongoDbMeta.getHostnames()));
        propertiesBuilder.set(MongoProp.PORT, vars.environmentSubstitute(mongoDbMeta.getPort()));
        propertiesBuilder.set(MongoProp.DBNAME, vars.environmentSubstitute(mongoDbMeta.getDbName()));
        propertiesBuilder.set(MongoProp.connectTimeout, mongoDbMeta.getConnectTimeout());
        propertiesBuilder.set(MongoProp.socketTimeout, mongoDbMeta.getSocketTimeout());
        propertiesBuilder.set(MongoProp.readPreference, mongoDbMeta.getReadPreference());
        propertiesBuilder.set(MongoProp.writeConcern, mongoDbMeta.getWriteConcern());
        propertiesBuilder.set(MongoProp.wTimeout, mongoDbMeta.getWTimeout());
        propertiesBuilder.set(MongoProp.JOURNALED, Boolean.toString(mongoDbMeta.getJournal()));
        propertiesBuilder.set(MongoProp.USE_ALL_REPLICA_SET_MEMBERS, Boolean.toString(mongoDbMeta.getUseAllReplicaSetMembers()));
        propertiesBuilder.set(MongoProp.USERNAME, mongoDbMeta.getAuthenticationUser());
        propertiesBuilder.set(MongoProp.PASSWORD, mongoDbMeta.getAuthenticationPassword());
        propertiesBuilder.set(MongoProp.USE_KERBEROS, Boolean.toString(mongoDbMeta.getUseKerberosAuthentication()));
        if(mongoDbMeta.getReadPrefTagSets() != null) {
            StringBuilder tagSet = new StringBuilder();
            for (String tag : mongoDbMeta.getReadPrefTagSets()) {
                tagSet.append(tag);
                tagSet.append(",");
            }
            // Remove trailing comma
            if (tagSet.length() > 0) {
                tagSet.setLength(tagSet.length() - 1);
            }
            propertiesBuilder.set(MongoProp.tagSet, tagSet.toString());
        }
        return MongoClientWrapperFactory.createMongoClientWrapper(propertiesBuilder.build(), new KettleMongoUtilLogger(log));
    }
}
