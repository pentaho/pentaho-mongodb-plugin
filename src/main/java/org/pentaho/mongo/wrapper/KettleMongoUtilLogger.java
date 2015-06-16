package org.pentaho.mongo.wrapper;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.mongo.MongoUtilLogger;

/**
 * Created by bryan on 8/7/14.
 */
public class KettleMongoUtilLogger implements MongoUtilLogger {
    private final LogChannelInterface logChannelInterface;

    public KettleMongoUtilLogger(LogChannelInterface logChannelInterface) {
        this.logChannelInterface = logChannelInterface;
    }

    @Override
    public void debug(String s) {
        if( logChannelInterface != null) {
            logChannelInterface.logDebug(s);
        }
    }

    @Override
    public void info(String s) {
        if( logChannelInterface != null) {
            logChannelInterface.logBasic(s);
        }
    }

    @Override
    public void warn(String s, Throwable throwable) {
        if( logChannelInterface != null) {
            logChannelInterface.logError(s, throwable);
        }
    }

    @Override
    public void error(String s, Throwable throwable) {
        if( logChannelInterface != null) {
            logChannelInterface.logError(s, throwable);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        if( logChannelInterface != null) {
            return logChannelInterface.isDebug();
        } else {
            return false;
        }
    }
}
