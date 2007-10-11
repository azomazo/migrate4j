package com.eroi.migrate.generators;

import com.eroi.migrate.DDLGenerator;
import com.eroi.migrate.SchemaElement;
import com.eroi.migrate.SchemaMigrationException;
import com.eroi.migrate.SchemaElement.Table;

public class H2Generator implements DDLGenerator {

	public String getCreateTableStatement(Table table) {
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
				SchemaElement.Column column = (SchemaElement.Column)columns[x];
				
				if (column instanceof SchemaElement.PrimaryKeyColumn) {
					retVal.append(makePrimaryKeyColumnString((SchemaElement.PrimaryKeyColumn)column));
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
	}
	
	private String makeColumnString(SchemaElement.Column column) {
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
		
		if (!column.isNullable()) {
			retVal.append(" NOT");;
		}
		retVal.append(" NULL ");
		
		if (column.getDefaultValue() != null) {
			retVal.append("DEFAULT '")
				  .append(column.getDefaultValue())
				  .append("' ");
		}
		
		retVal.append(",");
		
		return retVal.toString();
	}
	
	private String makePrimaryKeyColumnString(SchemaElement.PrimaryKeyColumn primaryKeyColumn) {
		StringBuffer retVal = new StringBuffer();
		
		retVal.append(makeColumnString(primaryKeyColumn).trim());
		
		//remove the comma
		retVal.delete(retVal.length() - 1, retVal.length());
		
		if (primaryKeyColumn.isAutoincrement()) {
			retVal.append("AUTO_INCREMENT ");
		}
		retVal.append("PRIMARY KEY");
				
		retVal.append(",");
		
		return retVal.toString();
	}

}
