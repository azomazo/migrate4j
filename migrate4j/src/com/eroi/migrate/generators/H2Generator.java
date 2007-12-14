package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public class H2Generator extends AbstractGenerator {

	public String addColumnStatement(Column column, Table table, int position) {
		// TODO Auto-generated method stub
		return null;
	}

	public String addIndex(Index index) {
		if (index == null) {
			throw new SchemaMigrationException("Invalid Index Object");
		}
		
		StringBuffer query = new StringBuffer();
		
		query.append("create ");
		
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
	
	public String createTableStatement(Table table, String options) {
		return createTableStatement(table);
	}
	
	public String createTableStatement(Table table) {
		
		StringBuffer retVal = new StringBuffer();
		
		Column[] columns = table.getColumns();
		
		if (columns == null || columns.length == 0) {
			throw new SchemaMigrationException("Table must include at least one column");
		}
		
		int numberOfKeyColumns = GeneratorHelper.countPrimaryKeyColumns(columns);
		if (numberOfKeyColumns != 1) {
			throw new SchemaMigrationException("Compound primary key support is not implemented yet.  Each table must have one and only one primary key.  You included " + numberOfKeyColumns);
		}
		
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
			throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
		}
		
		return retVal.toString().trim() + ");";
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
	          .append(makeColumnString(column));
	    
	    return retVal.toString();
	    
	}

	public String dropTableStatement(Table table) {
		if (table == null) {
			throw new SchemaMigrationException("Table must not be null");
		}
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("DROP TABLE ")
			  .append(wrapName(table.getTableName()));
	
		return retVal.toString();
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

	public String addForeignKey(ForeignKey foreignKey) {

		if (foreignKey == null) {
	        throw new SchemaMigrationException("Must include a non-null foreign key object");
	    }
	    
	    StringBuffer retVal = new StringBuffer();
	    
	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());
	    
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(foreignKey.getChildTable()))
	          .append(" add foreign key (")
	          .append(GeneratorHelper.makeStringList(childColumns))
	          .append(") references ")
	          .append(wrapName(foreignKey.getParentTable()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(parentColumns))
	          .append(")");
	    
	    return retVal.toString();
	}

	public String dropForeignKey(ForeignKey foreignKey) {
		// TODO Auto-generated method stub
		return null;
	}
	

	public boolean exists(ForeignKey foreignKey) {
		try {
			Connection connection = Configure.getConnection();
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getImportedKeys(null, "", foreignKey.getChildTable());
				
				if (resultSet != null) {
					while (resultSet.next()) {
						String parentTable = resultSet.getString("PKTABLE_NAME");
						String parentColumn = resultSet.getString("PKCOLUMN_NAME");
						String childColumn = resultSet.getString("FKCOLUMN_NAME");
						
						if (foreignKey.getParentTable().equals(parentTable) &&
								foreignKey.getParentColumns()[0].equals(parentColumn) &&
								foreignKey.getChildColumns()[0].equals(childColumn)) {
							return true;
						}
						
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
			return false;
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		}
	}

}
