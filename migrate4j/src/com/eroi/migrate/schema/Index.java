package com.eroi.migrate.schema;

import com.eroi.migrate.misc.SchemaMigrationException;

public class Index {

	private String name;
	private String tableName;
	private String[] columnNames;
	private boolean isUnique;
	private boolean isPrimaryKey;
	
	public Index(String tableName, String[] columnNames) {
		this(null, tableName, columnNames, false, false);
	}
	
	public Index(String name, String tableName, String[] columnNames, boolean isUnique, boolean isPrimaryKey) {
		this.name = name;
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.isUnique = isUnique;
		this.isPrimaryKey = isPrimaryKey;
		
		init();		
	}
	
	public String getName() {
		return name;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String[] getColumnNames() {
		return columnNames;
	}
	
	public boolean isUnique() {
		return isUnique;
	}
	
	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}
	
	private void init() {
		if (tableName == null || columnNames == null || columnNames.length == 0 || !hasColumns()) {
			throw new SchemaMigrationException("Must provide a table and columns to use for index");
		}
		
		if (name == null) {
			name = generateIndexName();
		}
	}

	private String generateIndexName() {
		StringBuffer name = new StringBuffer();
		
		name.append("idx_")
			.append(tableName.substring(0, 8))
			.append("_");
	
		if (columnNames.length == 1) {
			name.append(getCharacters(columnNames[0], 8));
		} else {
			//Multiple Columns
			name.append(getCharacters(columnNames[0], 4));
			name.append("_");
			name.append(getCharacters(columnNames[columnNames.length - 1], 4));
		}
		
		return name.toString();
	}

	private String getCharacters(String string, int length) {
		if (string == null) {
			return "null";
		}
		
		String str = string;
		
		if (str.length() > length) {
			str = str.substring(0, length);
		}
		
		return str;
	}
	
	private boolean hasColumns() {
		boolean hasColumn = false;
		
		for (int x = 0 ; x < columnNames.length ; x++) {
			if (columnNames[x] != null && columnNames[x].trim().length() > 0) {
				hasColumn = true;
				break;
			}
		}
		
		return hasColumn;
	}
	
}
