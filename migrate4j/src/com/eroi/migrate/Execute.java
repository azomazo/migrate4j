package com.eroi.migrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains commands that can be called from within Migration classes.
 *
 */
public class Execute {
	
	private static Log log = LogFactory.getLog(Execute.class);
	
	public static boolean exists(Index index) {
		Validator.notNull(index, "Index can not be null");
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(index);
			
		} catch (SQLException e) {
            log.error("Unable to check index " + index.getName() + " on table " + index.getTableName(), e);
			throw new SchemaMigrationException("Unable to check index " + index.getName() + " on table " + index.getTableName(), e);
		} 
	}
	
	public static boolean indexExists(String indexName, String tableName) {
		Validator.notNull(indexName, "Index name can not be null");
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.indexExists(indexName, tableName);
			
		} catch (SQLException e) {
			String message = "Unable to check index " + indexName + " on table " + tableName;
			log.error(message, e);
			throw new SchemaMigrationException(message, e);
		} 
	}
	
	public static boolean exists(Table table) {
		Validator.notNull(table, "Table can not be null");
		
		return tableExists(table.getTableName());
	}
	
	public static boolean tableExists(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.tableExists(tableName);
			
		} catch (SQLException e) {
			log.error("Unable to create table " + tableName, e);
			throw new SchemaMigrationException("Unable to check table " + tableName, e);
		}
	}
	
	public static boolean exists(Column column, Table table) {
		Validator.notNull(table, "Table can not be null");
		Validator.notNull(column, "Column can not be null");
		
		return exists(column.getColumnName(), table.getTableName());
		
	}
	
	public static boolean exists(Column column, String tableName) {
		Validator.notNull(column, "Column can not be null");
		
		return exists(column.getColumnName(), tableName);
		
	}
	
	public static boolean exists(String columnName, String tableName) {
		Validator.notNull(tableName, "Table Name can not be null");
		Validator.notNull(columnName, "Column Name can not be null");
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.columnExists(columnName, tableName);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to check column " + columnName + " on table " + tableName, e);
		} 
	}
	
	public static boolean exists(ForeignKey foreignKey) {
		Validator.notNull(foreignKey, "Foreign key can not be null");
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(foreignKey);
			
		} catch (SQLException e) {
			log.error("Unable to check foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to check foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
		} 
	}
	
	public static void createTable(Table table) {
		createTable(table, null);
	}
	
	public static void createTable(Table table, String tableOptions){
		Validator.notNull(table, "Table can not be null");
		
		if (exists(table)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
		
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
	
	public static void dropTable(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");
		
		if (!tableExists(tableName)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropTableStatement(tableName);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop table " + tableName, e);
			throw new SchemaMigrationException("Unable to drop table " + tableName, e);
		} 
	}
	
	public static void addColumn(Column column, Table table) {
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		Validator.isTrue(exists(table), "Table does not exist");
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addColumnStatement(column, table.getTableName(), null);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to alter table " + table.getTableName() + " and add column " + column.getColumnName(), e);
			throw new SchemaMigrationException("Unable to alter table " + table.getTableName() + " and add column " + column.getColumnName(), e);
		}
	}
	
	public static void dropColumn(Column column, Table table) {
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(table, "Table can not be null");
		Validator.isTrue(exists(table), "Table does not exist");
		
		if (!exists(column.getColumnName(), table.getTableName())) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropColumnStatement(column.getColumnName(), table.getTableName());
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to alter table " + table.getTableName() + " and drop column " + column.getColumnName(), e);
			throw new SchemaMigrationException("Unable to alter table " + table.getTableName() + " and drop column " + column.getColumnName(), e);
		}
		
	}
	
	public static void addIndex(Index index) {
		Validator.notNull(index, "Index can not be null");
		
		if (exists(index)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addIndex(index);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to add index " + index.getName() + " on table " + index.getTableName(), e);
			throw new SchemaMigrationException("Unable to add index " + index.getName() + " on table " + index.getTableName(), e);
		}
	}
	
	public static void dropIndex(Index index) {
		Validator.notNull(index, "Index can not be null");
		
		if (!exists(index)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropIndex(index);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop index " + index.getName() + " from table " + index.getTableName(), e);
			throw new SchemaMigrationException("Unable to drop index " + index.getName() + " from table " + index.getTableName(), e);
		}
	}
	
	public static void addForeignKey(ForeignKey foreignKey) {
		Validator.notNull(foreignKey, "ForeignKey can not be null");
		
		if (exists(foreignKey)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addForeignKey(foreignKey);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to add foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to add foreign key " + foreignKey.getName() + " on table " + foreignKey.getParentTable(), e);
		}
	}
	
	public static void dropForeignKey(ForeignKey foreignKey) {
		Validator.notNull(foreignKey, "ForeignKey can not be null");
		
		if (!exists(foreignKey)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropForeignKey(foreignKey);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			log.error("Unable to drop foreign key " + foreignKey.getName() + " from table " + foreignKey.getParentTable(), e);
			throw new SchemaMigrationException("Unable to drop foreign key " + foreignKey.getName() + " from table " + foreignKey.getParentTable(), e);
		}
	}
	
	public static void statement(Connection connection, String query) throws SQLException {
		executeStatement(connection, query);
	}
	
	private static void executeStatement(Connection connection, String query) throws SQLException {
		
		Statement statement = null;
		
		try {
			statement = connection.createStatement();
			statement.executeUpdate(query);
		} finally {
			Closer.close(statement);
		}
		
	}
}
