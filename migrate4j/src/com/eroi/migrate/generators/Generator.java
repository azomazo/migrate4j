package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;


/**
 * Responsible for creating SQL DDL statements for a specific database.
 *
 */
public interface Generator {

	/**
	 * Determines whether a table named <code>tableName</code> exists
	 * 
	 * @param tableName String name of table
	 * @return true if table exists, otherwise false
	 */
	public boolean tableExists(String tableName);
	
	public boolean columnExists(String columnName, String tableName);
	
	public boolean indexExists(String indexName, String tableName);
	
	public boolean exists(Index index);
	
	public boolean foreignKeyExists(String foreignKeyName, String childTableName);
	
	public boolean exists(ForeignKey foreignKey);
	
	public boolean hasPrimaryKey(String tableName);

	public boolean isPrimaryKey(String columnName, String tableName);

	public String dropPrimaryKey(String tableName);

	public String dropPrimaryKey(Table tableName);

	public String createTableStatement(Table table);
	
	public String createTableStatement(Table table, String options);

	public String dropTableStatement(String tableName);
	
	public String addColumnStatement(Column column, String tableName, String afterColumn);
	
	public String addColumnStatement(Column column, String tableName, int position);
	
	public String alterColumnStatement(Column definition, String tableName);
	
	public String dropColumnStatement(String columnName, String tableName);
	
	public String addIndex(Index index);
	
	public String dropIndex(Index index);
	
	public String dropIndex(String indexName, String tableName);
	
	public String addForeignKey(ForeignKey foreignKey);
	
	public String dropForeignKey(ForeignKey foreignKey);
	
	public String dropForeignKey(String foreignKeyName, String childTable);
	
	public String renameColumn(String newColumnName, String oldColumnName, String tableName);
	
	public String wrapName(String name);

	/**
	 * Some DBs need more than one statement to implement a column change. Therefore we let the 
	 * generator do it.
	 * The Generic Generator will however implement the single statement approach by calling the 
	 * {@link #alterColumn(Connection, Column, String)} method. So that DBs that only need onw statement
	 * can simply implement that method.
	 * 
	 * @param connection
	 */
	public void alterColumn(Column column, String tableName) throws SQLException ;
	
	public String renameTableStatement(String tableName, String newName);
}
