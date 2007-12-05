package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.schema.Column;

/**
 * Provides basic logic for most Generators.
 *
 */
public class GeneratorHelper {

	private static final Map types;
	private static final List needsLength;
	private static final List needsQuotes;
	private static final List stringTypes;
	
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
		types.put(new Integer(Types.LONGVARCHAR), "TEXT");
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
		needsQuotes.add(new Integer(Types.LONGVARCHAR));
		needsQuotes.add(new Integer(Types.VARCHAR));
		
		stringTypes = new ArrayList();
		stringTypes.add(new Integer(Types.CHAR));
		stringTypes.add(new Integer(Types.LONGVARCHAR));
		stringTypes.add(new Integer(Types.VARCHAR));
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
	
	public static boolean isStringType(int type) {
		return stringTypes.contains(new Integer(type));
	}
	
	public static int countPrimaryKeyColumns(Column[] columns) {
		int retVal = 0;
		
		for (int x = 0 ; x < columns.length ; x++) {
			
			try {
				Column column = columns[x];
				
				if (column != null && column.isPrimaryKey()) {
					retVal++;
				}
				
			} catch (ClassCastException ignored) {
			}
		}
		
		return retVal;
	}
	
	public static int countAutoIncrementColumns(Column[] columns) {
		int retVal = 0;
		
		for (int x = 0 ; x < columns.length ; x++) {
			
			try {
				Column column = columns[x];
				
				if (column != null && column.isAutoincrement()) {
					retVal++;
				}
				
			} catch (ClassCastException ignored) {
			}
		}
		
		return retVal;
	}
	
	public static boolean doesTableExist(Connection connection, String tableName) throws SQLException {
		ResultSet resultSet = null;
		
		try {
		
			DatabaseMetaData databaseMetaData = connection.getMetaData();
		
			String catalog = getCatalog(connection);
			resultSet = databaseMetaData.getTables(catalog, "", tableName, null);
			
			if (resultSet != null && resultSet.next()) {
				return true;
			}
		} finally {
			Closer.close(resultSet);
		}
		
		return false;
	}
	
	public static boolean doesColumnExist(Connection connection, String columnName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		
		try {
		
			DatabaseMetaData databaseMetaData = connection.getMetaData();
		
			String catalog = getCatalog(connection);
			resultSet = databaseMetaData.getColumns(catalog, "", tableName, columnName);
			
			if (resultSet != null && resultSet.next()) {
				return true;
			}
		} finally {
			Closer.close(resultSet);
		}
		
		return false;
	}
	
	public static boolean doesIndexExist(Connection connection, String indexName, String tableName) throws SQLException {
		ResultSet resultSet = null;
		
		try {
		
			DatabaseMetaData databaseMetaData = connection.getMetaData();
		
			String catalog = getCatalog(connection);
			resultSet = databaseMetaData.getIndexInfo(catalog, "", tableName, false, false);
			
			if (resultSet != null) {
				while (resultSet.next()) {
					String name = resultSet.getString("INDEX_NAME");
					if (name != null & name.equals(indexName)) {
						return true;
					}
				}
			}
		} finally {
			Closer.close(resultSet);
		}
		
		return false;
	}
	
	private static String getCatalog(Connection connection) throws SQLException {
		return connection.getCatalog();
	}
	
}
