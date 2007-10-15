package com.eroi.migrate;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;


public class Define {
	
	public static Column column(String columnName, int columnType) {
		return new Column(columnName, columnType);
	}

	public static Column column(String columnName,
							 	int columnType, 
								 int length, 
								 boolean primaryKey,
								 boolean nullable,
								 Object defaultValue, 
								 boolean autoincrement) {
		return new Column(columnName, 
						  columnType, 
						  length,
						  primaryKey,
						  nullable, 
						  defaultValue, 
						  autoincrement);
	}
	
	public static Table table(String tableName, Column[] columns) {
		return new Table(tableName, columns);
	}
	
}
