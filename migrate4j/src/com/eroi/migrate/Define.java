package com.eroi.migrate;

import java.sql.Types;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public class Define {

	public enum DataTypes {
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
    	
    	private DataTypes(int typeValue) { 
    		this.typeValue = typeValue;
    	}
    	
    	public int getTypeValue() {
    		return this.typeValue;
    	}
    	
    }
	
    public static Column column(String columnName, DataTypes columnType, ColumnOption<?> ... columnOption) {
    	Column column = new Column(columnName, columnType.getTypeValue());
    	
    	if (columnOption != null && columnOption.length > 0) {
    		for(ColumnOption<?> option : columnOption) {
    			if(option != null) {
    				option.decorate(column);
    			}
        	}
    	}

    	return column;
	}

    public static Table table(String tableName, Column... columns) {    	
    	return new Table(tableName, columns);
    }
    
    public static Index index(String indexName, String tableName, String... columnNames) {
    	return new Index(indexName, tableName, columnNames, false, false);
    }
    
    public static Index uniqueIndex(String indexName, String tableName, String... columnNames) {
    	return new Index(indexName, tableName, columnNames, true, false);
    }
    
    public static ForeignKey foreignKey(String name, String parentTable, String parentColumn, String childTable, String childColumn) {
    	return new ForeignKey(name, parentTable, parentColumn, childTable, childColumn);
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
