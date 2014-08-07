package org.pentaho.mongo.wrapper.field;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.bson.types.*;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.mongo.MongoDbException;
import org.pentaho.mongo.wrapper.MongoClientWrapper;
import org.pentaho.mongo.wrapper.MongoDBAction;

import java.util.*;

/**
 * Created by bryan on 8/7/14.
 */
public class FieldsUtil {
    private static final Class<?> PKG = FieldsUtil.class;
    public static List<MongoField> discoverFields( MongoClientWrapper wrapper, String db, final String collection, final String query, final String fields,
                                            final boolean isPipeline, final int docsToSample ) throws KettleException {
        try {
            return wrapper.perform(db, new MongoDBAction<List<MongoField>>() {
                @Override
                public List<MongoField> perform(DB db) throws MongoDbException {
                    DBCursor cursor = null;
                        int numDocsToSample = docsToSample;
                        if (numDocsToSample < 1) {
                            numDocsToSample = 100; // default
                        }

                        List<MongoField> discoveredFields = new ArrayList<MongoField>();
                        Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();
                        try {
                            if (Const.isEmpty(collection)) {
                                throw new KettleException(BaseMessages.getString(PKG,
                                        "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified")); //$NON-NLS-1$
                            }
                            DBCollection dbcollection = db.getCollection(collection);

                            Iterator<DBObject> pipeSample = null;

                            if (isPipeline) {
                                pipeSample = setUpPipelineSample(query, numDocsToSample, dbcollection);
                            } else {
                                if (Const.isEmpty(query) && Const.isEmpty(fields)) {
                                    cursor = dbcollection.find().limit(numDocsToSample);
                                } else {
                                    DBObject dbObject = (DBObject) JSON.parse(Const.isEmpty(query) ? "{}" //$NON-NLS-1$
                                            : query);
                                    DBObject dbObject2 = (DBObject) JSON.parse(fields);
                                    cursor = dbcollection.find(dbObject, dbObject2).limit(numDocsToSample);
                                }
                            }

                            int actualCount = 0;
                            while (cursor != null ? cursor.hasNext() : pipeSample.hasNext()) {
                                actualCount++;
                                DBObject nextDoc = (cursor != null ? cursor.next() : pipeSample.next());
                                docToFields(nextDoc, fieldLookup);
                            }

                            postProcessPaths(fieldLookup, discoveredFields, actualCount);

                            return discoveredFields;
                        } catch (Exception e) {
                            throw new MongoDbException(e);
                        } finally {
                            if (cursor != null) {
                                cursor.close();
                            }
                        }
                }
            });
        } catch (Exception ex) {
            if (ex instanceof KettleException) {
                throw (KettleException) ex;
            } else {
                throw new KettleException(BaseMessages.getString(PKG,
                        "MongoNoAuthWrapper.ErrorMessage.UnableToDiscoverFields"), ex); //$NON-NLS-1$
            }
        }
    }
    protected static void postProcessPaths( Map<String, MongoField> fieldLookup, List<MongoField> discoveredFields,
                                            int numDocsProcessed ) {
        for ( String key : fieldLookup.keySet() ) {
            MongoField m = fieldLookup.get( key );
            m.m_occurenceFraction = "" + m.m_percentageOfSample + "/" //$NON-NLS-1$ //$NON-NLS-2$
                    + numDocsProcessed;
            setMinArrayIndexes( m );

            // set field names to terminal part and copy any min:max array index
            // info
            if ( m.m_fieldName.contains( "[" ) && m.m_fieldName.contains( ":" ) ) { //$NON-NLS-1$ //$NON-NLS-2$
                m.m_arrayIndexInfo = m.m_fieldName;
            }
            if ( m.m_fieldName.indexOf( '.' ) >= 0 ) {
                m.m_fieldName = m.m_fieldName.substring( m.m_fieldName.lastIndexOf( '.' ) + 1, m.m_fieldName.length() );
            }

            if ( m.m_disparateTypes ) {
                // force type to string if we've seen this path more than once
                // with incompatible types
                m.m_kettleType = ValueMeta.getTypeDesc(ValueMeta.TYPE_STRING);
            }
            discoveredFields.add( m );
        }

        // check for name clashes
        Map<String, Integer> tempM = new HashMap<String, Integer>();
        for ( MongoField m : discoveredFields ) {
            if ( tempM.get( m.m_fieldName ) != null ) {
                Integer toUse = tempM.get( m.m_fieldName );
                String key = m.m_fieldName;
                m.m_fieldName = key + "_" + toUse; //$NON-NLS-1$
                toUse = new Integer( toUse.intValue() + 1 );
                tempM.put( key, toUse );
            } else {
                tempM.put( m.m_fieldName, 1 );
            }
        }
    }
    protected static void setMinArrayIndexes( MongoField m ) {
        // set the actual index for each array in the path to the
        // corresponding minimum index
        // recorded in the name

        if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
            return;
        }

        String temp = m.m_fieldPath;
        String tempComp = m.m_fieldName;
        StringBuffer updated = new StringBuffer();

        while ( temp.indexOf( '[' ) >= 0 ) {
            String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
            String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

            if ( !innerPart.equals( "-" ) ) { //$NON-NLS-1$
                // terminal primitive specific index
                updated.append( temp ); // finished
                temp = ""; //$NON-NLS-1$
                break;
            } else {
                updated.append( firstPart );

                String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

                if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
                    temp = temp.substring( temp.indexOf( ']' ) + 1, temp.length() );
                    tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1, tempComp.length() );
                } else {
                    temp = ""; //$NON-NLS-1$
                }

                String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
                String replace = "[" + compParts[0] + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                updated.append( replace );

            }
        }

        if ( temp.length() > 0 ) {
            // append remaining part
            updated.append( temp );
        }

        m.m_fieldPath = updated.toString();
    }
    protected static void docToFields( DBObject doc, Map<String, MongoField> lookup ) {
        String root = "$"; //$NON-NLS-1$
        String name = "$"; //$NON-NLS-1$

        if ( doc instanceof BasicDBObject ) {
            processRecord( (BasicDBObject) doc, root, name, lookup );
        } else if ( doc instanceof BasicDBList ) {
            processList( (BasicDBList) doc, root, name, lookup );
        }
    }
    private static void processRecord( BasicDBObject rec, String path, String name, Map<String, MongoField> lookup ) {
        for ( String key : rec.keySet() ) {
            Object fieldValue = rec.get( key );

            if ( fieldValue instanceof BasicDBObject ) {
                processRecord( (BasicDBObject) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
                        + key, lookup );
            } else if ( fieldValue instanceof BasicDBList ) {
                processList( (BasicDBList) fieldValue, path + "." + key, name + "." //$NON-NLS-1$ //$NON-NLS-2$
                        + key, lookup );
            } else {
                // some sort of primitive
                String finalPath = path + "." + key; //$NON-NLS-1$
                String finalName = name + "." + key; //$NON-NLS-1$
                if ( !lookup.containsKey( finalPath ) ) {
                    MongoField newField = new MongoField();
                    int kettleType = mongoToKettleType( fieldValue );
                    // Following suit of mongoToKettleType by interpreting null as String type
                    newField.m_mongoType = String.class;
                    if ( fieldValue != null ) {
                        newField.m_mongoType = fieldValue.getClass();
                    }
                    newField.m_fieldName = finalName;
                    newField.m_fieldPath = finalPath;
                    newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
                    newField.m_percentageOfSample = 1;

                    lookup.put( finalPath, newField );
                } else {
                    // update max indexes in array parts of name
                    MongoField m = lookup.get( finalPath );
                    Class<?> fieldClass = String.class;
                    if ( fieldValue != null ) {
                        fieldClass = fieldValue.getClass();
                    }
                    if ( !m.m_mongoType.isAssignableFrom( fieldClass ) ) {
                        m.m_disparateTypes = true;
                    }
                    m.m_percentageOfSample++;
                    updateMaxArrayIndexes( m, finalName );
                }
            }
        }
    }
    private static void processList( BasicDBList list, String path, String name, Map<String, MongoField> lookup ) {

        if ( list.size() == 0 ) {
            return; // can't infer anything about an empty list
        }

        String nonPrimitivePath = path + "[-]"; //$NON-NLS-1$
        String primitivePath = path;

        for ( int i = 0; i < list.size(); i++ ) {
            Object element = list.get( i );

            if ( element instanceof BasicDBObject ) {
                processRecord( (BasicDBObject) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
                        + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
            } else if ( element instanceof BasicDBList ) {
                processList( (BasicDBList) element, nonPrimitivePath, name + "[" + i //$NON-NLS-1$
                        + ":" + i + "]", lookup ); //$NON-NLS-1$ //$NON-NLS-2$
            } else {
                // some sort of primitive
                String finalPath = primitivePath + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                String finalName = name + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                if ( !lookup.containsKey( finalPath ) ) {
                    MongoField newField = new MongoField();
                    int kettleType = mongoToKettleType( element );
                    // Following suit of mongoToKettleType by interpreting null as String type
                    newField.m_mongoType = String.class;
                    if ( element != null ) {
                        newField.m_mongoType = element.getClass();
                    }
                    newField.m_fieldName = finalPath;
                    newField.m_fieldPath = finalName;
                    newField.m_kettleType = ValueMeta.getTypeDesc( kettleType );
                    newField.m_percentageOfSample = 1;

                    lookup.put( finalPath, newField );
                } else {
                    // update max indexes in array parts of name
                    MongoField m = lookup.get( finalPath );
                    Class<?> elementClass = String.class;
                    if ( element != null ) {
                        elementClass = element.getClass();
                    }
                    if ( !m.m_mongoType.isAssignableFrom( elementClass ) ) {
                        m.m_disparateTypes = true;
                    }
                    m.m_percentageOfSample++;
                    updateMaxArrayIndexes( m, finalName );
                }
            }
        }
    }
    protected static void updateMaxArrayIndexes( MongoField m, String update ) {
        // just look at the second (i.e. max index value) in the array parts
        // of update
        if (m.m_fieldName.indexOf('[') < 0) {
            return;
        }

        if (m.m_fieldName.split("\\[").length != update.split("\\[").length) { //$NON-NLS-1$ //$NON-NLS-2$
            throw new IllegalArgumentException("Field path and update path do not seem to contain " //$NON-NLS-1$
                    + "the same number of array parts!"); //$NON-NLS-1$
        }

        String temp = m.m_fieldName;
        String tempComp = update;
        StringBuffer updated = new StringBuffer();

        while (temp.indexOf('[') >= 0) {
            String firstPart = temp.substring(0, temp.indexOf('['));
            String innerPart = temp.substring(temp.indexOf('[') + 1, temp.indexOf(']'));

            if (innerPart.indexOf(':') < 0) {
                // terminal primitive specific index
                updated.append(temp); // finished
                temp = ""; //$NON-NLS-1$
                break;
            } else {
                updated.append(firstPart);

                String innerComp = tempComp.substring(tempComp.indexOf('[') + 1, tempComp.indexOf(']'));

                if (temp.indexOf(']') < temp.length() - 1) {
                    temp = temp.substring(temp.indexOf(']') + 1, temp.length());
                    tempComp = tempComp.substring(tempComp.indexOf(']') + 1, tempComp.length());
                } else {
                    temp = ""; //$NON-NLS-1$
                }

                String[] origParts = innerPart.split(":"); //$NON-NLS-1$
                String[] compParts = innerComp.split(":"); //$NON-NLS-1$
                int origMax = Integer.parseInt(origParts[1]);
                int compMax = Integer.parseInt(compParts[1]);

                if (compMax > origMax) {
                    // updated the max index seen for this path
                    String newRange = "[" + origParts[0] + ":" + compMax + "]"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                    updated.append(newRange);
                } else {
                    String oldRange = "[" + innerPart + "]"; //$NON-NLS-1$ //$NON-NLS-2$
                    updated.append(oldRange);
                }
            }
        }

        if (temp.length() > 0) {
            // append remaining part
            updated.append(temp);
        }

        m.m_fieldName = updated.toString();
    }
    protected static int mongoToKettleType( Object fieldValue ) {
        if ( fieldValue == null ) {
            return ValueMetaInterface.TYPE_STRING;
        }

        if ( fieldValue instanceof Symbol || fieldValue instanceof String || fieldValue instanceof Code
                || fieldValue instanceof ObjectId || fieldValue instanceof MinKey || fieldValue instanceof MaxKey) {
            return ValueMetaInterface.TYPE_STRING;
        } else if ( fieldValue instanceof Date ) {
            return ValueMetaInterface.TYPE_DATE;
        } else if ( fieldValue instanceof Number ) {
            // try to parse as an Integer
            try {
                Integer.parseInt( fieldValue.toString() );
                return ValueMetaInterface.TYPE_INTEGER;
            } catch ( NumberFormatException e ) {
                return ValueMetaInterface.TYPE_NUMBER;
            }
        } else if ( fieldValue instanceof Binary ) {
            return ValueMetaInterface.TYPE_BINARY;
        } else if ( fieldValue instanceof BSONTimestamp ) {
            return ValueMetaInterface.TYPE_INTEGER;
        }

        return ValueMetaInterface.TYPE_STRING;
    }
    private static Iterator<DBObject> setUpPipelineSample( String query, int numDocsToSample, DBCollection collection )
            throws KettleException {

        query = query + ", {$limit : " + numDocsToSample + "}"; //$NON-NLS-1$ //$NON-NLS-2$
        List<DBObject> samplePipe = jsonPipelineToDBObjectList( query );

        DBObject first = samplePipe.get( 0 );
        DBObject[] remainder = new DBObject[samplePipe.size() - 1];
        for ( int i = 1; i < samplePipe.size(); i++ ) {
            remainder[i - 1] = samplePipe.get( i );
        }

        AggregationOutput result = collection.aggregate( first, remainder );

        return result.results().iterator();
    }


    public static List<DBObject> jsonPipelineToDBObjectList( String jsonPipeline ) throws KettleException {
        List<DBObject> pipeline = new ArrayList<DBObject>();
        StringBuilder b = new StringBuilder( jsonPipeline.trim() );

        // extract the parts of the pipeline
        int bracketCount = -1;
        List<String> parts = new ArrayList<String>();
        int i = 0;
        while ( i < b.length() ) {
            if ( b.charAt( i ) == '{' ) {
                if ( bracketCount == -1 ) {
                    // trim anything off before this point
                    b.delete( 0, i );
                    bracketCount = 0;
                    i = 0;
                }
                bracketCount++;
            }
            if ( b.charAt( i ) == '}' ) {
                bracketCount--;
            }
            if ( bracketCount == 0 ) {
                String part = b.substring( 0, i + 1 );
                parts.add( part );
                bracketCount = -1;

                if ( i == b.length() - 1 ) {
                    break;
                }
                b.delete( 0, i + 1 );
                i = 0;
            }

            i++;
        }

        for ( String p : parts ) {
            if ( !Const.isEmpty( p ) ) {
                DBObject o = (DBObject) JSON.parse( p );
                pipeline.add( o );
            }
        }

        if ( pipeline.size() == 0 ) {
            throw new KettleException( BaseMessages.getString( PKG,
                    "MongoNoAuthWrapper.ErrorMessage.UnableToParsePipelineOperators" ) ); //$NON-NLS-1$
        }

        return pipeline;
    }
}
