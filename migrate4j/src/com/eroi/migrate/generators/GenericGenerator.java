package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public class GenericGenerator implements Generator {

	private static Log log = LogFactory.getLog(GenericGenerator.class);

	public String addColumnStatement(Column column, String tableName,
			int position) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String addColumnStatement(Column column, String tableName,
			String afterColumn) {

		Validator.notNull(column, "Column can not be null");
		Validator.notNull(tableName, "Table name can not be null");
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    retVal.append("alter table ")
	          .append(wrapName(tableName))
	          .append(" add ")
	          .append(makeColumnString(column));
	    
	    //This is based on having to pass a "before"
	    if (afterColumn != null && afterColumn.trim().length() > 0) {
	    	
	    	List<String> columnNames = getExistingColumnNames(tableName);
	    	Validator.notNull(columnNames, "Could not get existing columns to determine where to place new column");
	    	Validator.isTrue(columnNames.size() > 0, "Did not find any existing columns");
	    	
	    	String before = null;
	    	for (String col : columnNames) {
	    		if (col.equalsIgnoreCase(afterColumn)) {
	    			int index = columnNames.indexOf(col) + 1; 
	    			if (columnNames.size() >= index) {
	    				before = columnNames.get(index);
	    				break;
	    			} else if (columnNames.size() == index - 1) {
	    				//request was to add as last column
	    				before = "";
	    				break;
	    			}
	    		}
	    	}
	    	
	    	Validator.notNull(before, "Could not find " + afterColumn);
	    	
	    	if (before.trim().length() > 0){
	    	
	    		retVal.append(" before ")
	    			.append(before);
	    	}
	    }
	    
	    return retVal.toString();
	    
	}
	
	private List<String> getExistingColumnNames(String tableName) {
		
		List<String> columnNames = new ArrayList<String>();
		
		try {
			Connection connection = Configure.getConnection();
			
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getColumns(null, null, tableName, "%");
				
				if (resultSet != null) {
					while (resultSet.next()) {
						columnNames.add(resultSet.getString("COLUMN_NAME"));
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
		} catch (SQLException exception) {
			log.error(exception.getMessage(), exception);
			throw new SchemaMigrationException("Failed to get existing columns: " + exception.getMessage(), exception);
		}
		
		return columnNames; 
	}
	
	public String addForeignKey(ForeignKey foreignKey) {
		Validator.notNull(foreignKey, "Foreign key can not be null");
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());
	    
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(foreignKey.getChildTable()))
	          .append(" add constraint ")
	          .append(wrapName(foreignKey.getName()))
	          .append(" foreign key  (")
	          .append(GeneratorHelper.makeStringList(childColumns))
	          .append(") references ")
	          .append(wrapName(foreignKey.getParentTable()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(parentColumns))
	          .append(")");
	    
	    return retVal.toString();
	}
	
	
	/**
	 * create [unique] index <name> [primary key] on <table>(<column>[,<column>...])
	 */
	public String addIndex(Index index) {
		Validator.notNull(index, "Index can not be null");
		
		StringBuffer query = new StringBuffer("create ");
		
		if (index.isUnique()) {
			query.append("unique ");
		}
		
		query.append("index ")
			.append(wrapName(index.getName()))
			.append(" ");
		
		if (index.isPrimaryKey()) {
			query.append("primary key ");
		}
		
		query.append("on ")
			.append(wrapName(index.getTableName()))
			.append("(");
			
		String[] columns = index.getColumnNames();
		String comma = "";
		for (int x = 0 ; x < columns.length ; x++) {	
			query.append(comma)
				.append(wrapName(columns[x]));
			
			comma = ", ";
				
		}
		
		query.append(")");
		
		return query.toString();
	}
	
	
	public boolean columnExists(String columnName, String tableName) {
		try {
			Connection connection = Configure.getConnection();
			
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getColumns(null, null, tableName, columnName);
				
				if (resultSet != null && resultSet.next()) {
					return true;
				}
			} finally {
				Closer.close(resultSet);
			}
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Column , Table )!!",exception);
			throw new SchemaMigrationException(exception);
		}
		
		return false;
	}

	public String createTableStatement(Table table, String options) {
		return createTableStatement(table);
	}
	
	public String createTableStatement(Table table) {
		
		StringBuffer retVal = new StringBuffer();

		Validator.notNull(table, "Table can not be null");		
		
		Column[] columns = table.getColumns();

		Validator.notNull(columns, "Columns can not be null");
		Validator.isTrue(columns.length > 0, "At least one column must exist");
		
		int numberOfAutoIncrementColumns = GeneratorHelper.countAutoIncrementColumns(columns);
		
		Validator.isTrue(numberOfAutoIncrementColumns <=1, "Can not have more than one autoincrement key");
				
		retVal.append("create table ")
			  .append(wrapName(table.getTableName()))
			  .append(" (");
		
		try {
			for (int x = 0 ; x < columns.length ; x++ ){
				Column column = (Column)columns[x];
				
				if (x > 0) {
					retVal.append(", ");
				}
				
				retVal.append(makeColumnString(column));
				
			}
		} catch (ClassCastException e) {
			log.error("A table column couldn't be cast to a column: " + e.getMessage());
			throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
		}
		
		return retVal.toString().trim() + ");";
	}
	
	public String dropColumnStatement(String columnName, String tableName) {

		Validator.notNull(columnName, "Column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
	    StringBuffer query = new StringBuffer();
	    
	    query.append("alter table ")
	    	.append(wrapName(tableName))
	    	.append(" drop ")
	    	.append(wrapName(columnName));
	    
		return query.toString();
	}
	
	public String dropForeignKey(String foreignKeyName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String dropIndex(String indexName) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public String dropTableStatement(String tableName) {
		Validator.notNull(tableName, "Table name must not be null");
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("DROP TABLE ")
			  .append(wrapName(tableName));
	
		return retVal.toString();
	}
	
	public boolean exists(ForeignKey foreignKey) {
		
		Validator.notNull(foreignKey, "Foreign key can not be null");
		
		return foreignKeyExists(foreignKey.getName(), foreignKey.getChildTable());
	}
	
	public boolean foreignKeyExists(String foreignKeyName, String childTableName) {
		Validator.notNull(foreignKeyName, "Foreign key name can not be null");
		Validator.notNull(childTableName, "Child table name can not be null");
		
		try {
			Connection connection = Configure.getConnection();
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getImportedKeys(null, null, childTableName);
				
				if (resultSet != null) {
					while (resultSet.next()) {
						String parentTable = resultSet.getString("FK_NAME");
						
						if (foreignKeyName.equalsIgnoreCase(parentTable)) {
							return true;
						}
						
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
			return false;
		} catch (SQLException exception) {
                       log.error("Error occoured in H2Generator.exsists(ForeignKey)",exception);
			throw new SchemaMigrationException(exception);
		}
	}
	
	
	public boolean indexExists(String indexName, String tableName) {
		Validator.notNull(indexName, "Index name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Connection connection = Configure.getConnection();
			
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getIndexInfo(null, null, tableName, false, false);
				
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
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Index)!!",exception);
			throw new SchemaMigrationException(exception);
		}
	
		return false;
	}
	
	public static boolean doesTableExist(Connection connection, String tableName) throws SQLException {
		ResultSet resultSet = null;
		
		try {
		
			DatabaseMetaData databaseMetaData = connection.getMetaData();
		
			resultSet = databaseMetaData.getTables(null, null, tableName, null);
			
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
		
			resultSet = databaseMetaData.getColumns(null, null, tableName, columnName);
			
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
		
			resultSet = databaseMetaData.getIndexInfo(null, null, tableName, false, false);
			
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
	
	public boolean exists(Index index) {
		
		if (index.getName() != null && index.getName().trim().length() > 0) {
			return indexExists(index.getName(), index.getTableName());
		}
		
		throw new SchemaMigrationException("Can't determine if index exists without knowing it's name");
	}
	
	public boolean exists(String columnName, String tableName) {
		try {
			Connection connection = Configure.getConnection();
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getColumns(null, null, tableName, columnName);
				
				if (resultSet != null) {
					while (resultSet.next()) {
						String table = resultSet.getString("TABLE_NAME");
						String column = resultSet.getString("COLUMN_NAME");
						
						if (tableName.equalsIgnoreCase(table) &&
								columnName.equalsIgnoreCase(column)) {
							return true;
						}
						
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
			return false;
		} catch (SQLException exception) {
			log.error("Error occoured in H2Generator.exsists(ForeignKey)",exception);
			throw new SchemaMigrationException(exception);
		}
	}
	
	public boolean tableExists(String tableName) {
		try {
			Connection connection = Configure.getConnection();
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getTables(connection.getCatalog(), "", tableName, null);
				
				if (resultSet != null) {
					while (resultSet.next()) {
						if (tableName.equalsIgnoreCase(resultSet.getString("TABLE_NAME"))) {
							return true;
						}
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
			return false;
		} catch (SQLException exception) {
                       log.error("Error occoured in H2Generator.exsists(ForeignKey)",exception);
			throw new SchemaMigrationException(exception);
		}
	}
	
	public boolean exists(Column column, Table table) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesColumnExist(connection, column.getColumnName(), table.getTableName());
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Column , Table )!!",exception);
			throw new SchemaMigrationException(exception);
		}
	}

	public String dropColumnStatement(Column column, Table table) {
	
	    if (column == null) {
	    	log.debug("Could not locate Column in AbstractGenerator.dropColumnStatement()");
	        throw new SchemaMigrationException("Must include a non-null column");
	    }
	    
	    if (table == null) {
	    	log.debug("Could not locate Table in AbstractGenerator.dropColumnStatement()");
	        throw new SchemaMigrationException ("Must provide a table to drop the column from");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("alter table ")
	    	.append(wrapName(table.getTableName()))
	    	.append(" drop ")
	    	.append(wrapName(column.getColumnName()));
	    
		return query.toString();
	}
	
	public String dropIndex(Index index) {
		
	    if (index == null) {
                  log.debug("Null Index located in AbstractGenerator.dropIndex(Index)");
	        throw new SchemaMigrationException("Must include a non-null index");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("drop index ")
	    	.append(wrapName(index.getName()));
	    
		return query.toString();
	}
	
	public String dropForeignKey(ForeignKey foreignKey) {
		
		StringBuffer retVal = new StringBuffer();
		
		retVal.append("alter table ")
			.append(wrapName(foreignKey.getChildTable()))
			.append(" drop constraint ")
			.append(wrapName(foreignKey.getName()));
		
		return retVal.toString();
	}

	public String wrapName(String name) {
		StringBuffer wrap = new StringBuffer();
		
		wrap.append(getIdentifier())
			.append(name)
			.append(getIdentifier());
	
		return wrap.toString();
	}
	
	public String[] wrapStrings(String[] strings) {
		
		String[] wrapped = new String[strings.length];
		
		for (int x = 0 ; x < strings.length ; x++ ) {
			wrapped[x] = wrapName(strings[x]);
		}
		
		return wrapped;
	}
	
	protected String getIdentifier() {
		return "\"";
	}
	
	protected String makeColumnString(Column column) {
		StringBuffer retVal = new StringBuffer();
		
		retVal.append(wrapName(column.getColumnName()))
			  .append(" ");		
		
		int type = column.getColumnType();
		
		retVal.append(GeneratorHelper.getSqlName(type));
		if (GeneratorHelper.needsLength(type)) {
			
			retVal.append("(")
				  .append(column.getLength())
				  .append(")");
			
		}
		retVal.append(" ");
		
		if (!column.isNullable()) {
			retVal.append("NOT ");;
		}
		retVal.append("NULL ");
		
		if (column.isAutoincrement()) {
			retVal.append("AUTO_INCREMENT ");
		}
		
		if (column.isPrimaryKey()) {
			retVal.append("PRIMARY KEY ");
		}
		
		if (column.getDefaultValue() != null) {
			retVal.append("DEFAULT '")
				  .append(column.getDefaultValue())
				  .append("' ");
		}
		
		return retVal.toString().trim();
	}

}
