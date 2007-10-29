package com.eroi.migrate.generators;

import java.sql.Statement;

import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class SybaseGenerator extends AbstractGenerator {

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
		
		retVal.append("create table \"")
			  .append(table.getTableName())
			  .append("\" (");
		
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
	    
	    retVal.append("alter table \"")
	          .append(table.getTableName())
	          .append("\" add ")
	          .append(makeColumnString(column));
	    
	    return retVal.toString();
	    
	}



	public String dropTableStatement(Table table) {
		if (table == null) {
			throw new SchemaMigrationException("Table must not be null");
		}
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("DROP TABLE \"")
			  .append(table.getTableName())
			  .append("\"");
	
		return retVal.toString();
	}



	public String getStatement(Statement statement) {
		// TODO Auto-generated method stub
		return null;
	}
	
	protected String makeColumnString(Column column) {
		StringBuffer retVal = new StringBuffer();
		
		retVal.append("\"")
			  .append(column.getColumnName())
			  .append("\" ");		
		
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
			retVal.append("DEFAULT AUTOINCREMENT ");
		}
		
		if (column.getDefaultValue() != null) {
			retVal.append("DEFAULT '")
				  .append(column.getDefaultValue())
				  .append("' ");
		}
		
		if (column.isPrimaryKey()) {
                    retVal.append("PRIMARY KEY ");
                }
		
		return retVal.toString().trim();
	}

}
