package com.eroi.migrate.generators;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.eroi.migrate.SchemaElement;

/**
 * Provides basic logic for most Generators.
 *
 */
public class GeneratorHelper {

	private static final Map types;
	private static final List needsLength;
	private static final List needsQuotes;
	
	static {
		types = new HashMap();
		types.put(new Integer(Types.BIGINT), "BIGINT");
		types.put(new Integer(Types.BOOLEAN), "BOOL");
		types.put(new Integer(Types.CHAR), "CHAR");
		types.put(new Integer(Types.DATE), "DATE");
		types.put(new Integer(Types.DECIMAL), "DECIMAL");
		types.put(new Integer(Types.DOUBLE), "DOUBLE");
		types.put(new Integer(Types.FLOAT), "FLOAT");
		types.put(new Integer(Types.INTEGER), "INT");
		types.put(new Integer(Types.NUMERIC), "NUMERIC");
		types.put(new Integer(Types.SMALLINT), "SMALLINT");
		types.put(new Integer(Types.TIME), "TIME");
		types.put(new Integer(Types.TIMESTAMP), "TIMESTAMP");
		types.put(new Integer(Types.TINYINT), "TINYINT");
		types.put(new Integer(Types.VARCHAR), "VARCHAR");
		
		needsLength = new ArrayList();
		needsLength.add(new Integer(Types.CHAR));
		needsLength.add(new Integer(Types.VARCHAR));
		
		needsQuotes = new ArrayList();
		needsQuotes.add(new Integer(Types.CHAR));
		needsQuotes.add(new Integer(Types.VARCHAR));
	}
	
	public static String getSqlName(int type) {
		return (String)types.get(new Integer(type));
	}
	
	public static boolean needsLength(int type) {
		return needsLength.contains(new Integer(type));
	}
	
	public static boolean needsQuotes(int type) {
		return needsQuotes.contains(new Integer(type));
	}
	
	public static int countPrimaryKeyColumns(SchemaElement[] columns) {
		int retVal = 0;
		
		for (int x = 0 ; x < columns.length ; x++) {
			try {
				SchemaElement.PrimaryKeyColumn column = 
					(SchemaElement.PrimaryKeyColumn)columns[x];
				retVal++;
			} catch (ClassCastException ignored) {
			}
		}
		
		return retVal;
	}
	
}
