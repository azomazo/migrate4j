package com.eroi.migrate;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;


public class Define {

    /**
     * @param columnName
     * @param columnType
     * @return
     */
    public static Column column(String columnName, int columnType) {
        return new Column(columnName, columnType);
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

    /**
     * @param tableName
     * @param columns
     * @return
     */
    public static Table table(String tableName, Column[] columns) {
        return new Table(tableName, columns);
    }

}
