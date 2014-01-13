package org.pentaho.mongo.wrapper.field;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.Binary;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputData;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class MongoField implements Comparable<MongoField> {
  protected static Class<?> PKG = MongoField.class; // for i18n purposes

  /** The name the the field will take in the outputted kettle stream */
  public String m_fieldName = ""; //$NON-NLS-1$

  /** The path to the field in the Mongo object */
  public String m_fieldPath = ""; //$NON-NLS-1$

  /** The kettle type for this field */
  public String m_kettleType = ""; //$NON-NLS-1$

  /** User-defined indexed values for String types */
  public List<String> m_indexedVals;

  /**
   * Temporary variable to hold the min:max array index info for fields determined when sampling documents for
   * paths/types
   */
  public transient String m_arrayIndexInfo;

  /**
   * Temporary variable to hold the number of times this path was seen when sampling documents to determine paths/types.
   */
  public transient int m_percentageOfSample = -1;

  /**
   * Temporary variable to hold the num times this path was seen/num sampled documents. Note that numerator might be
   * larger than denominator if this path is encountered multiple times in an array within one document.
   */
  public transient String m_occurenceFraction = ""; //$NON-NLS-1$

  public transient Object m_mongoType;

  /**
   * Temporary variable used to indicate that this path occurs multiple times over the sampled documents and that the
   * types differ. In this case we should default to Kettle type String as a catch-all
   */
  public transient boolean m_disparateTypes;

  /** The index that this field is in the output row structure */
  public int m_outputIndex;

  private ValueMetaInterface m_tempValueMeta;

  private List<String> m_pathParts;
  private List<String> m_tempParts;

  public MongoField copy() {
    MongoField newF = new MongoField();
    newF.m_fieldName = m_fieldName;
    newF.m_fieldPath = m_fieldPath;
    newF.m_kettleType = m_kettleType;

    // reference doesn't matter here as this list is read only at runtime
    newF.m_indexedVals = m_indexedVals;

    return newF;
  }

  /**
   * Initialize this mongo field
   * 
   * @param outputIndex
   *          the index for this field in the outgoing row structure.
   * @throws KettleException
   *           if a problem occurs
   */
  public void init( int outputIndex ) throws KettleException {
    if ( Const.isEmpty( m_fieldPath ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoDbOutput.Messages.MongoField.Error.NoPathSet" ) ); //$NON-NLS-1$
    }

    if ( m_pathParts != null ) {
      return;
    }

    String fieldPath = MongoDbInputData.cleansePath( m_fieldPath );

    String[] temp = fieldPath.split( "\\." ); //$NON-NLS-1$
    m_pathParts = new ArrayList<String>();
    for ( String part : temp ) {
      m_pathParts.add( part );
    }

    if ( m_pathParts.get( 0 ).equals( "$" ) ) { //$NON-NLS-1$
      m_pathParts.remove( 0 ); // root record indicator
    } else if ( m_pathParts.get( 0 ).startsWith( "$[" ) ) { //$NON-NLS-1$

      // strip leading $ off of array
      String r = m_pathParts.get( 0 ).substring( 1, m_pathParts.get( 0 ).length() );
      m_pathParts.set( 0, r );
    }

    m_tempParts = new ArrayList<String>();
    m_tempValueMeta = ValueMetaFactory.createValueMeta( ValueMeta.getType( m_kettleType ) );
    m_outputIndex = outputIndex;
  }

  /**
   * Reset this field, ready for processing a new document
   * 
   * @param space
   *          variables to use
   */
  public void reset( VariableSpace space ) {
    // first clear because there may be stuff left over from processing
    // the previous mongo document object (especially if a path exited early
    // due to non-existent field or array index out of bounds)
    m_tempParts.clear();

    for ( String part : m_pathParts ) {
      m_tempParts.add( space.environmentSubstitute( part ) );
    }
  }

  /**
   * Perform Kettle type conversions for the Mongo leaf field value.
   * 
   * @param fieldValue
   *          the leaf value from the Mongo structure
   * @return an Object of the appropriate Kettle type
   * @throws KettleException
   *           if a problem occurs
   */
  public Object getKettleValue( Object fieldValue ) throws KettleException {

    switch ( m_tempValueMeta.getType() ) {
      case ValueMetaInterface.TYPE_BIGNUMBER:
        if ( fieldValue instanceof Number ) {
          fieldValue = BigDecimal.valueOf( ( (Number) fieldValue ).doubleValue() );
        } else if ( fieldValue instanceof Date ) {
          fieldValue = new BigDecimal( ( (Date) fieldValue ).getTime() );
        } else {
          fieldValue = new BigDecimal( fieldValue.toString() );
        }
        return m_tempValueMeta.getBigNumber( fieldValue );
      case ValueMetaInterface.TYPE_BINARY:
        if ( fieldValue instanceof Binary ) {
          fieldValue = ( (Binary) fieldValue ).getData();
        } else {
          fieldValue = fieldValue.toString().getBytes();
        }
        return m_tempValueMeta.getBinary( fieldValue );
      case ValueMetaInterface.TYPE_BOOLEAN:
        if ( fieldValue instanceof Number ) {
          fieldValue = new Boolean( ( (Number) fieldValue ).intValue() != 0 );
        } else if ( fieldValue instanceof Date ) {
          fieldValue = new Boolean( ( (Date) fieldValue ).getTime() != 0 );
        } else {
          fieldValue = new Boolean( fieldValue.toString().equalsIgnoreCase( "Y" ) //$NON-NLS-1$
              || fieldValue.toString().equalsIgnoreCase( "T" ) //$NON-NLS-1$
              || fieldValue.toString().equalsIgnoreCase( "1" ) ); //$NON-NLS-1$
        }
        return m_tempValueMeta.getBoolean( fieldValue );
      case ValueMetaInterface.TYPE_DATE:
        if ( fieldValue instanceof Number ) {
          fieldValue = new Date( ( (Number) fieldValue ).longValue() );
        } else if ( fieldValue instanceof Date ) {
          // nothing to do
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.DateConversion", //$NON-NLS-1$
              fieldValue.toString() ) );
        }
        return m_tempValueMeta.getDate( fieldValue );
      case ValueMetaInterface.TYPE_INTEGER:
        if ( fieldValue instanceof Number ) {
          fieldValue = new Long( ( (Number) fieldValue ).intValue() );
        } else if ( fieldValue instanceof Binary ) {
          byte[] b = ( (Binary) fieldValue ).getData();
          String s = new String( b );
          fieldValue = new Integer( s );
        } else {
          fieldValue = new Integer( fieldValue.toString() );
        }
        return m_tempValueMeta.getInteger( fieldValue );
      case ValueMetaInterface.TYPE_NUMBER:
        if ( fieldValue instanceof Number ) {
          fieldValue = new Double( ( (Number) fieldValue ).doubleValue() );
        } else if ( fieldValue instanceof Binary ) {
          byte[] b = ( (Binary) fieldValue ).getData();
          String s = new String( b );
          fieldValue = new Double( s );
        } else {
          fieldValue = new Double( fieldValue.toString() );
        }
        return m_tempValueMeta.getNumber( fieldValue );
      case ValueMetaInterface.TYPE_STRING:
        return m_tempValueMeta.getString( fieldValue );
      default:
        return null;
    }
  }

  /**
   * Convert a mongo record object to a Kettle field value (for the field defined by this path)
   * 
   * @param mongoObject
   *          the record to convert
   * @return the kettle field value
   * @throws KettleException
   *           if a problem occurs
   */
  public Object convertToKettleValue( BasicDBObject mongoObject ) throws KettleException {

    if ( mongoObject == null ) {
      return null;
    }

    if ( m_tempParts.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.MalformedPathRecord" ) ); //$NON-NLS-1$
    }

    String part = m_tempParts.remove( 0 );

    if ( part.charAt( 0 ) == '[' ) {
      // we're not expecting an array at this point - this document does not
      // contain our field
      return null;
    }

    if ( part.indexOf( '[' ) > 0 ) {
      String arrayPart = part.substring( part.indexOf( '[' ) );
      part = part.substring( 0, part.indexOf( '[' ) );

      // put the array section back into location zero
      m_tempParts.add( 0, arrayPart );
    }

    // part is a named field of this record
    Object fieldValue = mongoObject.get( part );
    if ( fieldValue == null ) {
      return null;
    }

    // what have we got
    if ( m_tempParts.size() == 0 ) {
      // we're expecting a leaf primitive - lets see if that's what we have
      // here...
      return getKettleValue( fieldValue );
    }

    if ( fieldValue instanceof BasicDBObject ) {
      return convertToKettleValue( ( (BasicDBObject) fieldValue ) );
    }

    if ( fieldValue instanceof BasicDBList ) {
      return convertToKettleValue( ( (BasicDBList) fieldValue ) );
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return null;
  }

  /**
   * Convert a mongo array object to a Kettle field value (for the field defined in this path)
   * 
   * @param mongoObject
   *          the array to convert
   * @return the kettle field value
   * @throws KettleException
   *           if a problem occurs
   */
  public Object convertToKettleValue( BasicDBList mongoList ) throws KettleException {

    if ( mongoList == null ) {
      return null;
    }

    if ( m_tempParts.size() == 0 ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoDbInput.ErrorMessage.MalformedPathArray" ) ); //$NON-NLS-1$
    }

    String part = m_tempParts.remove( 0 );
    if ( !( part.charAt( 0 ) == '[' ) ) {
      // we're expecting an array at this point - this document does not
      // contain our field
      return null;
    }

    String index = part.substring( 1, part.indexOf( ']' ) );
    int arrayI = 0;
    try {
      arrayI = Integer.parseInt( index.trim() );
    } catch ( NumberFormatException e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "MongoDbInput.ErrorMessage.UnableToParseArrayIndex", index ) ); //$NON-NLS-1$
    }

    if ( part.indexOf( ']' ) < part.length() - 1 ) {
      // more dimensions to the array
      part = part.substring( part.indexOf( ']' ) + 1, part.length() );
      m_tempParts.add( 0, part );
    }

    if ( arrayI >= mongoList.size() || arrayI < 0 ) {
      return null;
    }

    Object element = mongoList.get( arrayI );

    if ( element == null ) {
      return null;
    }

    if ( m_tempParts.size() == 0 ) {
      // we're expecting a leaf primitive - let's see if that's what we have
      // here...
      return getKettleValue( element );
    }

    if ( element instanceof BasicDBObject ) {
      return convertToKettleValue( ( (BasicDBObject) element ) );
    }

    if ( element instanceof BasicDBList ) {
      return convertToKettleValue( ( (BasicDBList) element ) );
    }

    // must mean we have a primitive here, but we're expecting to process more
    // path so this doesn't match us - return null
    return null;
  }

  @Override
  public int compareTo( MongoField comp ) {
    return m_fieldName.compareTo( comp.m_fieldName );
  }
}
