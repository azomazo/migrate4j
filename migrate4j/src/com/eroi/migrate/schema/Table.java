package com.eroi.migrate.schema;


public class Table {

	private String tableName;
        private String engineName;
	private Column[] columns;
	
        public Table(String tableName, Column[] columns){
            this(tableName, null, columns);
        }
	public Table(String tableName, String engineName, Column[] columns) {
		if (tableName == null) {
			throw new RuntimeException("Table must not have a null name");
		}
		
		if (columns == null || columns.length == 0) {
			throw new RuntimeException("Table must have at least one column");
		}
		
		this.tableName = tableName;
                this.engineName = engineName;
		this.columns = columns;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public String getEngineName() {
		return engineName;
	}
	
	public Column[] getColumns() {
		return columns;
	}
	
}
