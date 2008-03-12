package com.eroi.migrate.schema;

import com.eroi.migrate.misc.Validator;


public class Table {

	private String tableName;
	private Column[] columns;

	public Table(String tableName, Column[] columns){
		
		Validator.notNull(tableName, "String tableName cannot be null");

		if (columns == null || columns.length == 0) {
			throw new RuntimeException("Table must have at least one column");
		}

		this.tableName = tableName;
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public Column[] getColumns() {
		return columns;
	}

}
