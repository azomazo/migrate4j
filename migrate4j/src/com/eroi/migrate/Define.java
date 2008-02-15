package com.eroi.migrate;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;
import com.eroi.migrate.schema.Column.ColumnOption;
import com.eroi.migrate.schema.Column.columnTypes;


public class Define {

    /**
     * @param columnName
     * @param columnType
     * @return
     */
    public static Column column(String columnName, int columnType) {
        return new Column(columnName, columnType);
    }
    
    public static Column column(String columnName, columnTypes columnType) {
		return column(columnName, columnType, (ColumnOption<?>)null);
	}
       
    public static Column column(String columnName, columnTypes columnType, ColumnOption<?> ... columnOption) {
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
    
    
    /**
     * @param columnName
     * @param columnType
     * @param length
     * @param primaryKey
     * @param nullable
     * @param defaultValue
     * @param autoincrement
     * @return
     */
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

    public static Index index(String tableName, String columnName) {
    	return new Index(tableName, new String[] { columnName });
    }
    
    public static Index index(String tableName, String[] columnNames) {
    	return new Index(tableName, columnNames);
    }
    
    public static Index index(String indexName, String tableName, String[] columnNames) {
    	return new Index(indexName, tableName, columnNames, false, false);
    }
    
    public static Index uniqueIndex(String tableName, String columnName) {
    	return new Index(null, tableName, new String[] { columnName }, true, false);
    }
    
    public static Index uniqueIndex(String tableName, String[] columnNames) {
    	return new Index(null, tableName, columnNames, true, false);
    }
    
    public static ForeignKey foreignKey(String parentTable, String parentColumn, String childTable, String childColumn) {
    	return new ForeignKey(parentTable, parentColumn, childTable, childColumn);
    }

}
