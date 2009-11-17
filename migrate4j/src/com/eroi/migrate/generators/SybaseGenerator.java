package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public class SybaseGenerator extends GenericGenerator {

	private static final Log log = Log.getLog(SybaseGenerator.class);

	public SybaseGenerator(Connection aConnection) {
		super(aConnection);
	}

	public boolean indexExists(String indexName, String tableName) {
		
		Validator.notNull(indexName, "Index name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			String query = "select index_name from systable t " 
				+ " inner join sysidx x on "
				+ "t.table_id = x.table_id "
				+ "where t.table_name = ? ";
			
			statement = getConnection().prepareStatement(query);
			statement.setString(1, tableName);
			
			resultSet = statement.executeQuery();
			
			if (resultSet != null) {
				while (resultSet.next()) {
					String name = resultSet.getString(1);
					if (name != null && name.equals(indexName)) {
						return true;
					}
				}
			}
			
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		return false;
	}
	
	public String createTableStatement(Table table, String options) {
		return createTableStatement(table);
	}
	
	public String addIndex(Index index) {
		
		Validator.notNull(index, "Index cannot be null");
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    retVal.append("create ");
	    
	    if (index.isUnique()) {
	    	retVal.append("unique ");
	    }
	    
	    retVal.append("index ")
	    	  .append(wrapName(index.getName()))
	          .append(" on ")
	          .append(wrapName(index.getTableName()))
	          .append(" (");
	    
	    String[] columnNames = index.getColumnNames();
	    String comma = "";
	    for (int x = 0 ; x < columnNames.length ; x++) {
	    	retVal.append(comma)
	    		.append(wrapName(columnNames[x]));
	    	
	    	comma = ", ";
	    }
	    
	    retVal.append(")");
	    
	    return retVal.toString();
	    		
	}
	
	public boolean tableExists(String tableName) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = getConnection().prepareStatement("select * from sysobjects where name = ?");
			statement.setString(1, tableName);
			
			resultSet = statement.executeQuery();
			
			if (resultSet != null && resultSet.next()) {
				return true;
			}
			
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		return false;
	}
	
	public boolean exists(Table table) {
		
		Validator.notNull(table, "Table can not be null");
		
		return tableExists(table.getTableName());
	}
	

	public boolean exists(String columnName, String tableName) {
		return columnExists(columnName, tableName);
	}
	
	public boolean columnExists(String columnName, String tableName) {
		
		Validator.notNull(columnName, "Column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			String query = "select * from systable t inner join systabcol c "
				+ "on t.table_id = c.table_id where table_name = ? and "
				+ "column_name = ? ";
			
			statement = getConnection().prepareStatement(query);
			statement.setString(1, tableName);
			statement.setString(2, columnName);
			
			resultSet = statement.executeQuery();
			
			if (resultSet != null && resultSet.next()) {
				return true;
			}
			
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		return false;
	}
	
	public boolean exists(Column column, Table table) {
		
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		
		return columnExists(column.getColumnName(), table.getTableName());
	}
	
	public boolean exists(Index index) {
		
		Validator.notNull(index, "Index can not be null");
		
		return indexExists(index.getName(), index.getTableName());

	}

	public boolean foreignKeyExists(String foreignKeyName, String childTableName) {
		
		Validator.notNull(foreignKeyName, "Foreign key name can not be null");
		
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			String query = "select * from sysforeignkeys where role = ? ";
			
			statement = getConnection().prepareStatement(query);
			statement.setString(1, foreignKeyName);
			
			resultSet = statement.executeQuery();
			
			if (resultSet != null && resultSet.next()) {
				return true;
			}
			
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		return false;
	}
	
	public boolean exists(ForeignKey foreignKey) {
		
		Validator.notNull(foreignKey, "Foreign key can not be null");
		return foreignKeyExists(foreignKey.getName(), foreignKey.getChildTable());
	}
	
	public String createTableStatement(Table table) {
		
		StringBuffer retVal = new StringBuffer();
		
		Column[] columns = table.getColumns();
		
		if (columns == null || columns.length == 0) {
			throw new SchemaMigrationException("Table must include at least one column");
		}
			
		if (GeneratorHelper.countAutoIncrementColumns(columns) > 1) {
			throw new SchemaMigrationException("Each table can have at most one auto_increment key.  You included " + GeneratorHelper.countAutoIncrementColumns(columns));
		}
		
		retVal.append("create table ")
			  .append(wrapName(table.getTableName()))
			  .append(" (");
		
		boolean hasMultiplePrimaryKeys = GeneratorHelper.countPrimaryKeyColumns(columns) > 1;
		
		try {
			for (int x = 0 ; x < columns.length ; x++ ){
				Column column = (Column)columns[x];
				
				if (x > 0) {
					retVal.append(", ");
				}
				
				retVal.append(makeColumnString(column, hasMultiplePrimaryKeys));
				
			}
		} catch (ClassCastException e) {
			throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
		}
		
		if (hasMultiplePrimaryKeys) {
			retVal.append(", primary key (");
		
			Column[] primaryKeys = GeneratorHelper.getPrimaryKeyColumns(columns);
			for (int x = 0; x < primaryKeys.length; x++) {
				Column column = (Column)primaryKeys[x];
		
				if (x > 0) {
				    retVal.append(", ");
				}
		
				retVal.append(wrapName(column.getColumnName()));
		    }
			
			retVal.append(") ");
		}
		
		retVal.append(")");
		
		log.debug("Creating table with query: " + retVal.toString());
		
		return retVal.toString();
	}

	public String addColumnStatement(Column column, Table table, String afterColumn) {
		return addColumnStatement(column, table.getTableName(), null);
	}
	
	public String addColumnStatement(Column column, Table table, int position) {
		return addColumnStatement(column, table.getTableName(), null);
	}

	public String addColumnStatement(Column column, String tableName,
			int position) {
		return addColumnStatement(column, tableName, null);
	}
	
	public String addColumnStatement(Column column, String tableName,
			String afterColumn) {

		Validator.notNull(column, "Column cannot be null");
	    
		Validator.notNull(tableName, "Table name cannot be null");
		
	    StringBuffer retVal = new StringBuffer();
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(tableName))
	          .append(" add ")
	          .append(makeColumnString(column, false));
	    
	    //After column doesn't seem to be an option for SQL Anywhere 10
	    
	    return retVal.toString();
	}
	

	public String dropTableStatement(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("DROP TABLE ")
			  .append(wrapName(tableName));
	
		return retVal.toString();
	}

	protected String makeColumnString(Column column, boolean suppressPrimaryKey) {
		StringBuffer retVal = new StringBuffer();
		
		retVal.append(wrapName(column.getColumnName()))
			  .append(" ");		
		
		int type = column.getColumnType();
		
		if (type == Types.BOOLEAN) {
			retVal.append(GeneratorHelper.getSqlName(Types.TINYINT));
		} else if (GeneratorHelper.isStringType(type) && column.isUnicode()){
			retVal.append("N").append(GeneratorHelper.getSqlName(type));
		} else {
			retVal.append(GeneratorHelper.getSqlName(type));
		}
		
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
			retVal.append("DEFAULT AUTOINCREMENT ");
		}
		
		if (column.getDefaultValue() != null) {
			retVal.append("DEFAULT '")
				  .append(column.getDefaultValue())
				  .append("' ");
		}
		
		if (!suppressPrimaryKey && column.isPrimaryKey()) {
			retVal.append("PRIMARY KEY ");
		}
		
		return retVal.toString().trim();
	}

	public String addForeignKey(ForeignKey foreignKey) {
		
		Validator.notNull(foreignKey, "ForeignKey cannot be null");
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());
	    
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(foreignKey.getChildTable()))
	          .append(" add foreign key ")
	          .append(wrapName(foreignKey.getName()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(childColumns))
	          .append(") references ")
	          .append(wrapName(foreignKey.getParentTable()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(parentColumns))
	          .append(")");
	    
	    return retVal.toString();
		
	}

	/**
	 * RENAME TABLE
	 */
	public String renameTableStatement(String tableName, String newName) {
		Validator.notNull(tableName, "Table name must not be null");
		Validator.notNull(newName, "new Table name must not be null");
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("ALTER TABLE ")
			.append(wrapName(tableName))
			.append(" RENAME ")
			.append(wrapName(newName));
	
		return retVal.toString();
	}
	
}
