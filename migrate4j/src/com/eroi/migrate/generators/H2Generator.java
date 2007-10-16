package com.eroi.migrate.generators;

import java.sql.Statement;

import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class H2Generator implements Generator {

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
		// TODO Auto-generated method stub
		return null;
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
	
/*	public String getTableStatement(Tabl table) {
		StringBuffer retVal = new StringBuffer();
		
		SchemaElement.Column[] columns = table.getColumns();
		
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
				SchemaElement.Column column = (com.eroi.migrate.schema.Column)columns[x];
				
				if (column instanceof SchemaElement.PrimaryKeyColumn) {
					retVal.append(makePrimaryKeyColumnString((com.eroi.migrate.schema.PrimaryKeyColumn)column));
				} else {
					retVal.append(makeColumnString(column));
				}
			}
		} catch (ClassCastException e) {
			throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
		}
		
		retVal.delete(retVal.length() - 2, retVal.length());
		
		return retVal.toString().trim() + ");";
	}
	
	public String getDropStatement(SchemaElement element) {
		StringBuffer retVal = new StringBuffer();
		
		retVal.append("DROP ")
			  .append(element.getSchemaTypeName())
			  .append(" \"")
			  .append(element.getSchemaObjectName())
			  .append("\"");
		
		return retVal.toString();
	}*/
	
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
