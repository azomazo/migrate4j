package com.eroi.migrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

/**
 * Contains commands that can be called from within Migration classes.
 *
 */
public class Execute {

	private static Log log = Log.getLog(Execute.class);
	
	/**
	 * Indicates whether an index exists
	 * 
	 * @param index
	 * @return
	 */
	public static boolean exists(Index index) {
		Validator.notNull(index, "Index can not be null");
		
		return indexExists(index.getName(), index.getTableName());
	}

	/**
	 * Indicates whether an index exists
	 * 
	 * @param connection
	 * @param index
	 * @return
	 */
	public static boolean exists(Connection connection, Index index) {
		return indexExists(connection, index.getName(), index.getTableName());
	}
	
	/**
	 * Indicates whether an index exists
	 * 
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public static boolean indexExists(String indexName, String tableName) {
		return indexExists(Configure.getConnection(), indexName, tableName);
	}
	
	/**
	 * Indicates whether an index exists
	 *
	 * @param connection
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public static boolean indexExists(Connection connection, String indexName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(indexName, "Index name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.indexExists(indexName, tableName);
			
		} catch (SQLException e) {
			String message = "Unable to check index " + indexName + " on table " + tableName;
			log.error(message, e);
			throw new SchemaMigrationException(message, e);
		} 
	}

	/**
	 * Indicates whether a table exists
	 * 
	 * @param table
	 * @return
	 */
	public static boolean exists(Table table) {
		Validator.notNull(table, "Table can not be null");
		
		return tableExists(table.getTableName());
	}

	/**
	 * Indicates whether a table exists
	 * 
	 * @param connection
	 * @param table
	 * @return
	 */
	public static boolean exists(Connection connection, Table table) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(table, "Table can not be null");
		
		return tableExists(connection, table.getTableName());
	}
	
	/**
	 * Indicates whether a table exists
	 * 
	 * @param tableName
	 * @return
	 */
	public static boolean tableExists(String tableName) {
		return tableExists(Configure.getConnection(), tableName);
	}

	/**
	 * Indicates whether a table exists
	 *
	 * @param connection
	 * @param tableName
	 * @return
	 */
	public static boolean tableExists(Connection connection, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.tableExists(tableName);
			
		} catch (SQLException e) {
			log.error("Unable to create table " + tableName, e);
			throw new SchemaMigrationException("Unable to check table " + tableName, e);
		}
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param column
	 * @param table
	 * @return
	 */
	public static boolean exists(Column column, Table table) {
		Validator.notNull(table, "Table can not be null");
		Validator.notNull(column, "Column can not be null");
		
		return columnExists(column.getColumnName(), table.getTableName());
		
	}

	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param connection
	 * @param column
	 * @param table
	 * @return
	 */
	public static boolean exists(Connection connection, Column column, Table table) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(table, "Table can not be null");
		Validator.notNull(column, "Column can not be null");
		
		return columnExists(connection, column.getColumnName(), table.getTableName());
		
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param column
	 * @param tableName
	 * @return
	 */
	public static boolean exists(Column column, String tableName) {
		Validator.notNull(column, "Column can not be null");
		
		return columnExists(column.getColumnName(), tableName);
		
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param conenction
	 * @param column
	 * @param tableName
	 * @return
	 */
	public static boolean exists(Connection connection, Column column, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(column, "Column can not be null");
		
		return columnExists(connection, column.getColumnName(), tableName);
		
	}

	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	public static boolean columnExists(String columnName, String tableName) {
		return columnExists(Configure.getConnection(), columnName, tableName);
	}

	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param connection
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	public static boolean columnExists(Connection connection, String columnName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(tableName, "Table Name can not be null");
		Validator.notNull(columnName, "Column Name can not be null");
		
		if (! tableExists(connection, tableName)) {
			return false;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.columnExists(columnName, tableName);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to check column " + columnName + " on table " + tableName, e);
		} 
	}

	/**
	 * Indicates whether a foreign key exists
	 * 
	 * @param foreignKeyName
	 * @param childTableName
	 * @return
	 */
	public static boolean foreignKeyExists(String foreignKeyName, String childTableName) {
		return foreignKeyExists(Configure.getConnection(), foreignKeyName, childTableName);
	}
	
	/**
	 * Indicates whether a foreign key exists
	 *
	 * @param connection
	 * @param foreignKeyName
	 * @param childTableName
	 * @return
	 */
	public static boolean foreignKeyExists(Connection connection, String foreignKeyName, String childTableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(foreignKeyName, "Foreign key name can not be null");
		Validator.notNull(childTableName, "Child table name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.foreignKeyExists(foreignKeyName, childTableName);
			
		} catch (SQLException e) {
			log.error("Unable to check foreign key " + foreignKeyName + " on table " + childTableName, e);
			throw new SchemaMigrationException("Unable to check foreign key " + foreignKeyName + " on table " + childTableName, e);
		} 
	}

	/**
	 * Indicates whether a foreign key exists
	 * 
	 * @param foreignKey
	 * @return
	 */
	public static boolean exists(ForeignKey foreignKey) {
		return exists(Configure.getConnection(), foreignKey);
	}

	/**
	 * Indicates whether a foreign key exists
	 * 
	 * @param connection
	 * @param foreignKey
	 * @return
	 */
	public static boolean exists(Connection connection, ForeignKey foreignKey) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(foreignKey, "Foreign key can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(foreignKey);
			
		} catch (SQLException e) {
			log.error("Unable to check foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to check foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
		} 
	}
	
	/**
	 * Returns true iff the given table contains a primary key
	 * 
	 * @param table
	 * @return
	 */
	public static boolean hasPrimaryKey(Table table) {
		Validator.notNull(table, "Table can not be null");
		return hasPrimaryKey(table.getTableName());
	}

	/**
	 * Returns true iff the given table (defined by its name) contains a primary key
	 * 
	 * @param tableName
	 * @return
	 */
	public static boolean hasPrimaryKey(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");

		return hasPrimaryKey(Configure.getConnection(), tableName);
	}
	
	/**
	 * Returns true iff the given table (defined by its name) contains a primary key
	 * 
	 * @param connection
	 * @param table
	 * @return
	 */
	public static boolean hasPrimaryKey(Connection connection, Table table) {
		Validator.notNull(table, "Table can not be null");
		return hasPrimaryKey(connection, table.getTableName());
	}

	/**
	 * Returns true iff the given table (defined by its name) contains a primary key
	 * 
	 * @param connection
	 * @param tableName
	 * @return
	 */
	public static boolean hasPrimaryKey(Connection connection, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.hasPrimaryKey(tableName);
			
		} catch (SQLException e) {
			String errMsg = String.format("Unable to check table %s for primary key", tableName);
			log.error(errMsg, e);
			throw new SchemaMigrationException(errMsg, e);
		} 

	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param table
	 * @param column
	 * @return
	 */
	public static boolean isPrimaryKey(Column column, Table table) {
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		
		return isPrimaryKey(column.getColumnName(), table.getTableName());
	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	public static boolean isPrimaryKey(String columnName, String tableName) {
		Validator.notNull(columnName, "Column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		return isPrimaryKey(Configure.getConnection(), columnName, tableName);
	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param connection
	 * @param column
	 * @param table
	 * @return
	 */
	public static boolean isPrimaryKey(Connection connection, Column column, Table table) {
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		
		return isPrimaryKey(connection, column.getColumnName(), table.getTableName());
	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param connection
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	public static boolean isPrimaryKey(Connection connection, String columnName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");		
		Validator.notNull(columnName, "Column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.isPrimaryKey(tableName, columnName);
			
		} catch (SQLException e) {
			String errMsg = String.format("Unable to check column %s for for primary key constraint in table %s", columnName, tableName);
			log.error(errMsg, e);
			throw new SchemaMigrationException(errMsg, e);
		} 
	}

	/**	 
	 * Drop a primary Key
	 * 
	 * @param table
	 */
	public static void dropPrimaryKey(Table table) {
		Validator.notNull(table, "Table can not be null");

		dropPrimaryKey(Configure.getConnection(), table.getTableName());
	}

	/**
	 * Drop a primary Key
	 * 
	 * @param tableName
	 */
	public static void dropPrimaryKey(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");

		dropPrimaryKey(Configure.getConnection(), tableName);
	}

	/**	 
	 * Drop a primary Key
	 * 
	 * @param connection
	 * @param table
	 */
	public static void dropPrimaryKey(Connection connection, Table table) {
		Validator.notNull(table, "Table can not be null");

		dropPrimaryKey(connection, table.getTableName());
	}

	/**
	 * Drop a primary Key
	 * 
	 * @param connection
	 * @param tableName
	 */
	public static void dropPrimaryKey(Connection connection, String tableName) {
		Validator.notNull(connection, "Connection can not be null");		
		Validator.notNull(tableName, "Table name can not be null");
	
		try {
			if (! hasPrimaryKey(connection, tableName)) {
				return;
			}
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropPrimaryKey(tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			String errMsg = String.format("Unable to drop primary key constraint from table %s", tableName);
			log.error(errMsg, e);
			throw new SchemaMigrationException(errMsg, e);
		} 
	}

	/**
	 * Create a table
	 * 
	 * @param table
	 */
	public static void createTable(Table table) {
		createTable(table, null);
	}
	
	/**
	 * 
	 * @param connection
	 * @param table
	 */
	public static void createTable(Connection connection, Table table) {
		createTable(connection, table, null);
	}
	
	/**
	 * Create a table with database specific options.
	 * This allows, for example, passing an engine type
	 * to MySQL.  While the <code>tableOptions</code>
	 * may be ignored for database products that do not
	 * accept such things, be aware that using this 
	 * argument may make your migrations no longer cross
	 * product compatible.
	 * 
	 * @param table
	 * @param tableOptions
	 */
	public static void createTable(Table table, String tableOptions){
		createTable(Configure.getConnection(), table, tableOptions);
	}
	
	/**
	 * Create a table with database specific options.
	 * This allows, for example, passing an engine type
	 * to MySQL.  While the <code>tableOptions</code>
	 * may be ignored for database products that do not
	 * accept such things, be aware that using this 
	 * argument may make your migrations no longer cross
	 * product compatible.
	 *
	 * @param connection
	 * @param table
	 * @param tableOptions
	 */
	public static void createTable(Connection connection, Table table, String tableOptions){
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(table, "Table can not be null");
		
		if (exists(connection, table)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query;
			if (tableOptions != null) {
				query = generator.createTableStatement(table, tableOptions);
			} else {
				query = generator.createTableStatement(table);
			}
			
			executeStatement(connection, query);
			
		} catch (SQLException e) {
			log.error("Unable to create table " + table.getTableName(), e);
			throw new SchemaMigrationException("Unable to create table " + table.getTableName(), e);
		} 
	}
	
	/**
	 * Drop a table
	 * 
	 * @param tableName
	 */
	public static void dropTable(String tableName) {
		dropTable(Configure.getConnection(), tableName);
	}
	
	/**
	 * Drop a table
	 * 
	 * @param connection
	 * @param tableName
	 */
	public static void dropTable(Connection connection, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		if (! tableExists(connection, tableName)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropTableStatement(tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop table " + tableName, e);
			throw new SchemaMigrationException("Unable to drop table " + tableName, e);
		} 
	}

	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param tableName
	 */
	public static void addColumn(Column column, String tableName) {
		addColumn(column, tableName, null);
	}

	/**
	 * Add a column to a table
	 * 
	 * @param connection
	 * @param column
	 * @param tableName
	 */
	public static void addColumn(Connection connection, Column column, String tableName) {
		addColumn(connection, column, tableName, null);
	}
	
	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param table
	 * @param afterColumn
	 */
	public static void addColumn(Column column, String table, String afterColumn) {
		addColumn(Configure.getConnection(), column, table, afterColumn);
	}

	/**
	 * Add a column to a table
	 * 
	 * @param connection
	 * @param column
	 * @param table
	 * @param afterColumn
	 */
	public static void addColumn(Connection connection, Column column, String table, String afterColumn) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		Validator.isTrue(tableExists(connection, table), "Table does not exist");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addColumnStatement(column, table, afterColumn);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to alter table " + table + " and add column " + column.getColumnName(), e);
			throw new SchemaMigrationException("Unable to alter table " + table + " and add column " + column.getColumnName(), e);
		}
	}
	
	/**
	 * Alter Column
	 * 
	 * @param column
	 * @param tableName
	 */
	public static void alterColumn(Column column, String tableName) {
		alterColumn(Configure.getConnection(), column, tableName);
	}
	
	/**
	 * Alter Column
	 * 
	 * @param connection
	 * @param column
	 * @param tableName
	 */
	public static void alterColumn(Connection connection, Column column, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			// HB: We let the generator do the work. 
			generator.alterColumn(column,tableName);

		} catch (SQLException e) {
			log.error("Unable to alter table " + tableName + " and alter column " + column.getColumnName(), e);
			throw new SchemaMigrationException("Unable to alter table " + tableName + " and alter column " + column.getColumnName(), e);
		}
	}

	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param table
	 * @param position
	 */
	public static void addColumn(Column column, String table, int position) {
		addColumn(Configure.getConnection(), column, table, position);
	}
	
	/**
	 * Add a column to a table
	 * 
	 * @param connection
	 * @param column
	 * @param table
	 * @param position
	 */
	public static void addColumn(Connection connection, Column column, String table, int position) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		Validator.isTrue(tableExists(connection, table), "Table does not exist");
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addColumnStatement(column, table, position);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to alter table " + table + " and add column " + column.getColumnName(), e);
			throw new SchemaMigrationException("Unable to alter table " + table + " and add column " + column.getColumnName(), e);
		}
	}
	
	/**
	 * Drop a column from a table
	 * 
	 * @param columnName
	 * @param tableName
	 */
	public static void dropColumn(String columnName, String tableName) {
		dropColumn(Configure.getConnection(), columnName, tableName);
	}
	
	/**
	 * Drop a column from a table
	 * 
	 * @param connection
	 * @param columnName
	 * @param tableName
	 */
	public static void dropColumn(Connection connection, String columnName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(columnName, "Column can not be null");
		Validator.notNull(tableName, "Table can not be null");
				
		if (! columnExists(connection, columnName, tableName)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropColumnStatement(columnName, tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to alter table " + tableName + " and drop column " + columnName, e);
			throw new SchemaMigrationException("Unable to alter table " + tableName + " and drop column " + columnName, e);
		}
		
	}

	/**
	 * Add an index
	 * 
	 * @param index
	 */
	public static void addIndex(Index index) {
		addIndex(Configure.getConnection(), index);
	}
	
	/**
	 * Add an index
	 *
	 * @param connection
	 * @param index
	 */
	public static void addIndex(Connection connection, Index index) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(index, "Index can not be null");
		
		if (exists(connection, index)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addIndex(index);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to add index " + index.getName() + " on table " + index.getTableName(), e);
			throw new SchemaMigrationException("Unable to add index " + index.getName() + " on table " + index.getTableName(), e);
		}
	}
	
	/**
	 * Drop an index
	 * 
	 * @param indexName
	 * @param tableName
	 */
	public static void dropIndex(String indexName, String tableName) {
		dropIndex(Configure.getConnection(), indexName, tableName);
	}

	/**
	 * Drop an index
	 * 
	 * @param connection
	 * @param indexName
	 * @param tableName
	 */
	public static void dropIndex(Connection connection, String indexName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(indexName, "Index can not be null");
		
		if (! indexExists(connection, indexName, tableName)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropIndex(indexName, tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop index " + indexName + " from table " + tableName, e);
			throw new SchemaMigrationException("Unable to drop index " + indexName + " from table " + tableName, e);
		}
	}
	
	/**
	 * Add a foreign key
	 * 
	 * @param foreignKey
	 */
	public static void addForeignKey(ForeignKey foreignKey) {
		addForeignKey(Configure.getConnection(), foreignKey);
	}

	/**
	 * Add a foreign key
	 * 
	 * @param connection
	 * @param foreignKey
	 */
	public static void addForeignKey(Connection connection, ForeignKey foreignKey) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(foreignKey, "ForeignKey can not be null");
		
		if (exists(connection, foreignKey) || ! tableExists(connection, foreignKey.getChildTable()) || ! tableExists(connection, foreignKey.getParentTable())) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addForeignKey(foreignKey);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to add foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to add foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
		}
	}
	
	/**
	 * Drop a foreign key
	 * 
	 * @param foreignKey
	 */
	public static void dropForeignKey(ForeignKey foreignKey) {
		dropForeignKey(Configure.getConnection(), foreignKey);
	}

	/**
	 * Drop a foreign key
	 * 
	 * @param connection
	 * @param foreignKey
	 */
	public static void dropForeignKey(Connection connection, ForeignKey foreignKey) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(foreignKey, "ForeignKey can not be null");
		
		if (! exists(connection, foreignKey)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropForeignKey(foreignKey);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop foreign key " + foreignKey.getName() + " from table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to drop foreign key " + foreignKey.getName() + " from table " + foreignKey.getParentTable(), e);
		}
	}
	
	/**
	 * Drop a foreign key
	 * 
	 * @param foreignKeyName
	 * @param childTableName
	 */
	public static void dropForeignKey(String foreignKeyName, String childTableName) {
		dropForeignKey(Configure.getConnection(), foreignKeyName, childTableName);
	}

	/**
	 * Drop a foreign key
	 * 
	 * @param connection
	 * @param foreignKeyName
	 * @param childTableName
	 */
	public static void dropForeignKey(Connection connection, String foreignKeyName, String childTableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(foreignKeyName, "ForeignKey can not be null");
		
		if (! foreignKeyExists(connection,  foreignKeyName, childTableName)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropForeignKey(foreignKeyName, childTableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop foreign key " + foreignKeyName + " from table " + childTableName, e);
			throw new SchemaMigrationException("Unable to drop foreign key " + foreignKeyName + " from table " + childTableName, e);
		}
		
	}
	
	/**
	 * Rename a column
	 * 
	 * @param newColumnName
	 * @param oldColumnName
	 * @param tableName
	 */
	public static void renameColumn(String newColumnName, String oldColumnName, String tableName) {
		renameColumn(Configure.getConnection(), newColumnName, oldColumnName, tableName);
	}

	/**
	 * Rename a column
	 * 
	 * @param connection
	 * @param newColumnName
	 * @param oldColumnName
	 * @param tableName
	 */
	public static void renameColumn(Connection connection, String newColumnName, String oldColumnName, String tableName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(newColumnName, "New column name can not be null");
		Validator.notNull(oldColumnName, "Old column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		if (! columnExists(connection, oldColumnName, tableName) || columnExists(connection, newColumnName, tableName)) {
			//We must have already done this
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.renameColumn(newColumnName, oldColumnName, tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			String message = "Unable to rename column " + oldColumnName + " to " + newColumnName + " on table " + tableName;
			log.error(message, e);
			throw new SchemaMigrationException(message, e);
		}
	}
	
	/**
	 * Rename a table
	 * 
	 * @param tableName
	 * @param newName
	 */
	public static void renameTable(String tableName,	String newName) {
		renameTable(Configure.getConnection(), tableName, newName);
	}

	/**
	 * Rename a table
	 * 
	 * @param connection
	 * @param tableName
	 * @param newName
	 */
	public static void renameTable(Connection connection, String tableName,	String newName) {
		Validator.notNull(connection, "Connection can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		Validator.notNull(newName, "new Table name can not be null");
		
		if (! tableExists(connection, tableName) || tableName.equals(newName)) {
			return;
		}
		
		try {
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.renameTableStatement(tableName, newName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop table " + tableName, e);
			throw new SchemaMigrationException("Unable to drop table " + tableName, e);
		} 
	}
	
	public static int executeStatement(Connection connection, String query) throws SQLException {
		
		Statement statement = null;
		
		try {
			statement = connection.createStatement();
			return statement.executeUpdate(query);
		} finally {
			Closer.close(statement);
		}
		
	}
	
	public static String wrapName(Connection connection, String name) {
		try {
			Generator g = GeneratorFactory.getGenerator(connection);
			return g.wrapName(name);
		} catch (SQLException e) {
			log.error("Unable to wrap name");
			throw new SchemaMigrationException("Unable to wrap name", e);
		}
	}
}
