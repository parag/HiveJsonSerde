package org.apache.hadoop.hive.contrib.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

public class NewJson 
{
	/**
     * Apache commons logger
     */
    private static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());

    /**
     * The number of columns in the table this SerDe is being used with
     */
    private int numColumns;

    /**
     * List of column names in the table
     */
    private List<String> columnNames;


    /**
     * A row object
     */
    private ArrayList<Object> row;

    /**
     * Initialize this SerDe with the system properties and table properties
     * 
     */
    public void initialize(String columns)
	    throws SerDeException {
	LOG.debug("Initializing JsonSerde");

	columnNames = Arrays.asList(columns.split(","));
	
	numColumns = columnNames.size();

	// Create ObjectInspectors from the type information for each column
	List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
		columnNames.size());

	// Create an empty row object to be reused during deserialization
	row = new ArrayList<Object>(numColumns);
	for (int c = 0; c < numColumns; c++) {
	    row.add(null);
	}

	LOG.debug("JsonSerde initialization complete");
    }

    /**
     * Gets the ObjectInspector for a row deserialized by this SerDe
     */
    public ObjectInspector getObjectInspector() throws SerDeException {
	return null;
    }

    /**
     * Deserialize a JSON Object into a row for the table
     */
    public Object deserialize(String blob) throws SerDeException {
	String rowText =  blob;
	LOG.debug("Deserialize row: " + rowText.toString());

	// Try parsing row into JSON object
	JSONObject jObj;
	try {
	    jObj = new JSONObject(rowText.toString());
	} catch (JSONException e) {
	    // If row is not a JSON object, make the whole row NULL
	    LOG.error("Row is not a valid JSON Object - JSONException: "
		    + e.getMessage());
	    return null;
	}

	// Loop over columns in table and set values
	String colName;
	Object value;
	for (int c = 0; c < numColumns; c++) {
	    colName = columnNames.get(c);
	    try {
		value = jObj.get(colName);
	    } catch (JSONException e) {
		// If the column cannot be found, just make it a NULL value and
		// skip over it
		LOG.warn("Column '" + colName + "' not found in row: "
			+ rowText.toString() + " - JSONException: "
			+ e.getMessage());
		value = null;
	    }
	    row.set(c, value);
	}

	return row;
    }

}
