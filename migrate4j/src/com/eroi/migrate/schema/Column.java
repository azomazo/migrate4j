package com.eroi.migrate.schema;


public class Column {

	private String columnName;
	private int columnType;
	private int length;
	private boolean primaryKey;
	private boolean nullable;
	private Object defaultValue;
	private boolean autoincrement;
	
	public Column(String columnName, int columnType) {
		this(columnName, columnType, -1, false, true, null, false);
	}
	
	public Column(String columnName, 
				  int columnType, 
				  int length, 
				  boolean primaryKey,
				  boolean nullable, 
				  Object defaultValue, 
				  boolean autoincrement) {
		this.columnName = columnName;
		this.columnType = columnType;
		this.length = length;
		this.primaryKey = primaryKey;
		this.nullable = nullable;
		this.defaultValue = defaultValue;
		this.autoincrement = autoincrement;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public int getColumnType() {
		return columnType;
	}

	public int getLength() {
		return length;
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}
	
	public boolean isNullable() {
		return nullable;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}

	public boolean isAutoincrement() {
		return autoincrement;
	}
}