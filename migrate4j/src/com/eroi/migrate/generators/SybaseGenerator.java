package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
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

public class SybaseGenerator extends GenericGenerator {

	private static final Log log = LogFactory.getLog(SybaseGenerator.class);
	
	public String addColumnStatement(Column column, Table table, int position) {
		// TODO Auto-generated method stub
		return null;
	}

	public String addColumnStatement(Column column, String tableName,
			int position) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String addColumnStatement(Column column, String tableName,
			String afterColumn) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean columnExists(String columnName, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String dropColumnStatement(String columnName, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String dropForeignKey(String foreignKeyName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String dropIndex(String indexName) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean exists(String columnName, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean foreignKeyExists(String foreignKeyName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean foreignKeyExists(String parentTableName,
			List<String> parentColumnNames, String childTable,
			List<String> childColumnNames) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean indexExists(String indexName, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean tableExists(String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String createTableStatement(Table table, String options) {
		return createTableStatement(table);
	}
	
	public String addIndex(Index index) {
		
	    if (index == null) {
	        throw new SchemaMigrationException("Must include a non-null index");
	    }
	    
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
	
	public boolean exists(Table table) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			Connection connection = Configure.getConnection();
			
			statement = connection.prepareStatement("select * from sysobjects where name = ?");
			statement.setString(1, table.getTableName());
			
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
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			Connection connection = Configure.getConnection();
			
			String query = "select * from systable t inner join systabcol c "
				+ "on t.table_id = c.table_id where table_name = ? and "
				+ "column_name = ? ";
			
			statement = connection.prepareStatement(query);
			statement.setString(1, table.getTableName());
			statement.setString(2, column.getColumnName());
			
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
	
	public boolean exists(Index index) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			Connection connection = Configure.getConnection();
			
			String query = "select index_name from systable t " 
				+ " inner join sysidx x on "
				+ "t.table_id = x.table_id "
				+ "where t.table_name = ? ";
			
			statement = connection.prepareStatement(query);
			statement.setString(1, index.getTableName());
			
			resultSet = statement.executeQuery();
			
			if (resultSet != null) {
				while (resultSet.next()) {
					String name = resultSet.getString(1);
					if (name != null && name.equals(index.getName())) {
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
	

	public boolean exists(ForeignKey foreignKey) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		
		try {
			Connection connection = Configure.getConnection();
			
			String query = "select * from sysforeignkeys where role = ? ";
			
			statement = connection.prepareStatement(query);
			statement.setString(1, foreignKey.getName());
			
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
	//create table statement
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
		
	    if (column == null) {
	        throw new SchemaMigrationException("Must include a non-null column");
	    }
	    
	    if (table == null) {
	        throw new SchemaMigrationException ("Must provide a table to add the column too");
	    }
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(table.getTableName()))
	          .append(" add ")
	          .append(makeColumnString(column, false));
	    
	    return retVal.toString();
	    
	}

	public String dropTableStatement(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("DROP TABLE ")
			  .append(wrapName(tableName));
	
		return retVal.toString();
	}



	public String getStatement(Statement statement) {
		// TODO Auto-generated method stub
		return null;
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
		
		if (foreignKey == null) {
	        throw new SchemaMigrationException("Must include a non-null foreign key object");
	    }
	    
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

}
