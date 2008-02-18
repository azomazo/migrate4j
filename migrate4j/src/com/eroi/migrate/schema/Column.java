package com.eroi.migrate.schema;

import java.sql.Types;


public class Column {

	private String columnName;
	private int columnType;
	private int length;
	private boolean primaryKey;
	private boolean nullable;
	private Object defaultValue;
	private boolean autoincrement;
	private boolean unicode; 
	
	public enum columnTypes {
    	BIT(Types.BIT), 
    	TINYINT(Types.TINYINT), 
    	SMALLINT(Types.SMALLINT), 
    	INTEGER(Types.INTEGER), 
    	BIGINT(Types.BIGINT), 
    	FLOAT(Types.FLOAT), 
    	REAL(Types.REAL), 
    	DOUBLE(Types.DOUBLE), 
    	NUMERIC(Types.NUMERIC), 
    	DECIMAL(Types.DECIMAL), 
    	CHAR(Types.CHAR), 
    	VARCHAR(Types.VARCHAR), 
    	LONGVARCHAR(Types.LONGVARCHAR), 
    	DATE(Types.DATE), 
    	TIME(Types.TIME), 
    	TIMESTAMP(Types.TIMESTAMP), 
    	BINARY(Types.BINARY), 
    	VARBINARY(Types.VARBINARY), 
    	LONGVARBINARY(Types.LONGVARBINARY),  
    	BLOB(Types.BLOB), 
    	CLOB(Types.CLOB),  
    	BOOLEAN(Types.BOOLEAN);
    	
    	private int typeValue;
    	
    	private columnTypes(int typeValue) { 
    		this.typeValue = typeValue;
    	}
    	
    	public int getTypeValue() {
    		return this.typeValue;
    	}
    	
    }
	
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
		
		unicode = false;
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
	
	public void setLength(Integer length) {
		this.length = length;
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}
	
	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}
	
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean isAutoincrement() {
		return autoincrement;
	}
	
	public void setAutoIncrement(boolean autoincrement) {
		this.autoincrement = autoincrement;
	}
	
	public boolean isUnicode() {
		return unicode;
	}
	
	public void setUnicode(boolean unicode) {
		this.unicode = unicode;
	}
	
	
	public interface ColumnOption<T> {
		public void decorate(Column column);
	}
	
	
	public static class NotNull implements ColumnOption<Boolean> {
		private Boolean notNull;
		
		public NotNull(Boolean notnull) {
			notNull = notnull;
		}

		public NotNull() {
			this(true);
		}		

		public void decorate(Column column) {
			column.setNullable(!notNull);
		}
	}
	
	public static NotNull notnull(Boolean notnull) {
		return new NotNull(notnull);
	}
	
	public static NotNull notnull() {
		return notnull(true);
	}
	
	
	public static class AutoIncrement implements ColumnOption<Boolean> {
		private Boolean autoincrement;
		
		public AutoIncrement(Boolean isAutoincrement) {
			autoincrement = isAutoincrement;
		}
		
		public AutoIncrement() {
			this(true);
		}
					
		public void decorate(Column column) {
			column.setAutoIncrement(autoincrement);
		}
	}

	public static AutoIncrement autoincrement(Boolean isAutoincrement) {
		return new AutoIncrement(isAutoincrement);
	}
	
	public static AutoIncrement autoincrement() {
		return autoincrement(true);
	}
	
	
	public static class Unicode implements ColumnOption<Boolean> {
		private Boolean isUnicode;
		
		public Unicode(Boolean unicode) {
			isUnicode = unicode;
		}
		
		public Unicode() {
			this(true);
		}
					
		public void decorate(Column column) {
			column.setUnicode(isUnicode);
		}
	}

	public static Unicode unicode(Boolean isUnicode) {
		return new Unicode(isUnicode);
	}
	
	public static Unicode unicode() {
		return unicode(true);
	}
	
	
	public static class PrimaryKey implements ColumnOption<Boolean> {
		private Boolean isPrimaryKey;
		
		public PrimaryKey(Boolean primarykey) {
			isPrimaryKey = primarykey;
		}
		
		public PrimaryKey() {
			this(true);
		}
						
		public void decorate(Column column) {
			column.setPrimaryKey(isPrimaryKey);
		}
	}

	public static PrimaryKey primarykey(Boolean isPrimary) {
		return new PrimaryKey(isPrimary);
	}
	
	public static PrimaryKey primarykey() {
		return primarykey(true);
	}
	
	
	public static class Length implements ColumnOption<Integer> {
		private Integer myLength;
		
		public Length(Integer myLen) {
			myLength = myLen;
		}
			
		public void decorate(Column column) {
			column.setLength(myLength);
		}
		
	}

	public static Length length(Integer len) {
		return new Length(len);
	}
	
	
	public static class DefaultValue implements ColumnOption<Object> {
		private Object defaultObject;
		
		public DefaultValue(Object obj) {
			defaultObject = obj;
		}
		
		public void decorate(Column column) {
			column.setDefaultValue(defaultObject);
		}
	}
	
	public static DefaultValue defaultValue(Object obj) {
		return new DefaultValue(obj);
	}
	
}
