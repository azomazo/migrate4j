package com.eroi.migrate;

import java.util.Map;

public interface SchemaElement {
	
	public String getSchemaTypeName();
	public String getSchemaObjectName();
	
	public class PrimaryKeyColumn extends Column {
		
		private boolean autoincrement;
		
		public PrimaryKeyColumn(String columnName, int columnType, boolean autoincrement) {
			super(columnName, columnType, -1, false, null, null);
			this.autoincrement = autoincrement;
		}
		
		public PrimaryKeyColumn(String columnName, int columnType, int length, boolean nullable, Object defaultValue, String afterColumn, boolean autoincrement) {
			super(columnName, columnType, length, nullable, defaultValue, afterColumn);
			this.autoincrement = autoincrement;
		}
		
		public boolean isAutoincrement() {
			return autoincrement;
		}
		
	}
	
	public class Column implements SchemaElement {
		
		private String columnName;
		private int columnType;
		private int length;
		private boolean nullable;
		private Object defaultValue;
		private String afterColumn;
		
		public Column(String columnName, int columnType) {
			this(columnName, columnType, -1, true, null, null);
		}
		
		public Column(String columnName, int columnType, int length, boolean nullable, Object defaultValue, String afterColumn) {
			this.columnName = columnName;
			this.columnType = columnType;
			this.length = length;
			this.nullable = nullable;
			this.defaultValue = defaultValue;
			this.afterColumn = afterColumn;
		}
		
		public String getSchemaObjectName() {
			return columnName;
		}

		public String getSchemaTypeName() {
			return "COLUMN";
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

		public boolean isNullable() {
			return nullable;
		}

		public Object getDefaultValue() {
			return defaultValue;
		}

		public String getAfterColumn() {
			return afterColumn;
		}		
		
	}
	
	public class Table implements SchemaElement {
		
		private String tableName;
		private SchemaElement.Column[] columns;
		
		public Table(String tableName, SchemaElement.Column[] columns) {
			this.tableName = tableName;
			this.columns = columns;
		}
		
		public String getSchemaTypeName() {
			return "TABLE";
		}
		
		public String getSchemaObjectName() {
			return tableName;
		}
		
		public String getTableName() {
			return tableName;
		}
		
		public SchemaElement.Column[] getColumns() {
			return columns;
		}
		
	}
	
	/**
	 * Defines either a stored procedure or generic SQL statement.
	 * During {@link com.eroi.migrate.Migration#up() up} migrations,
	 * the <code>procName</code> argment is ignored and the 
	 * <code>statement</code> argument is executed in it's 
	 * entirety without any modification - do not assume "CREATE PROC"
	 * and <code>procName</code> will be prepended to the statement.
	 * During {@link com.eroi.migrate.Migration#down() down} migrations,
	 * the <code>statement</code> is ignored and a "DROP PROC" plus
	 * <code>procName</code> is executed.
	 *
	 */
	public class Procedure implements SchemaElement {
		
		private String procName;
		private String statement;
		private Map arguments;
		
		public Procedure(String procName, String statement, Map arguments) {
			this.procName = procName;
			this.statement = statement;
			this.arguments = arguments;
		}
		
		public String getSchemaTypeName() {
			return "PROCEDURE";
		}
		
		public String getSchemaObjectName() {
			return procName;
		}
		
		public String getStatement() {
			return statement;
		};
		
		public Map getArguments() {
			return arguments;
		}
		
	}
	
	public class Drop implements SchemaElement {
		
		private SchemaElement element;
		
		public Drop(SchemaElement element) {
			this.element = element;
		}
		
		public SchemaElement getElement() {
			return element;
		}
		
		public String getSchemaObjectName() {
			return element.getSchemaObjectName();
		}
		
		public String getSchemaTypeName() {
			return element.getSchemaTypeName();
		}
	}
}
