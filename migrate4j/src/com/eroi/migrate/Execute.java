package com.eroi.migrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public class Execute {

	public static boolean exists(Index index) {
		if (index == null) {
			throw new SchemaMigrationException("Invalid index object");
		}
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(index);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to check index " + index.getName() + " on table " + index.getTableName(), e);
		} 
	}
	
	public static boolean exists(Table table) {
		if (table == null) {
			throw new SchemaMigrationException("Invalid table object");
		}
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(table);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to check table " + table.getTableName(), e);
		} 
	}
	
	public static boolean exists(Column column, Table table) {
		if (table == null) {
			throw new SchemaMigrationException("Invalid table object");
		}
		
		if (column == null) {
			throw new SchemaMigrationException("Invalid column object");
		}
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			return generator.exists(column, table);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to check column " + column.getColumnName() + " on table " + table.getTableName(), e);
		} 
	}
	
	public static void createTable(Table table){
		
		if (table == null) {
			throw new SchemaMigrationException("Invalid table object");
		}
		
		if (exists(table)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
		
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.createTableStatement(table);
			
			executeStatement(connection, query);
			
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to create table " + table.getTableName(), e);
		} 
	}
	
	public static void dropTable(Table table) {
		if (table == null) {
			throw new SchemaMigrationException("Invalid Table object");
		}
		
		if (!exists(table)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropTableStatement(table);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to drop table " + table.getTableName(), e);
		} 
	}
	
	public static void addColumn(Column column, Table table) {
		if (table == null || column == null) {
			throw new SchemaMigrationException("Must provide a Table and Column");
		}
		
		if (!exists(table)) {
			throw new SchemaMigrationException("Table does not exist");
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addColumnStatement(column, table, null);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to alter table " + table.getTableName() + " and add column " + column.getColumnName(), e);
		}
	}
	
	public static void dropColumn(Column column, Table table) {
		if (table == null || column == null) {
			throw new SchemaMigrationException("Must provide a Table and Column");
		}
		
		if (!exists(table)) {
			throw new SchemaMigrationException("Table does not exist");
		}
		
		if (!exists(column, table)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropColumnStatement(column, table);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to alter table " + table.getTableName() + " and drop column " + column.getColumnName(), e);
		}
		
	}
	
	public static void addIndex(Index index) {
		if (index == null) {
			throw new SchemaMigrationException("Invalid Index Object");
		}
		
		if (exists(index)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.addIndex(index);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to add index " + index.getName() + " on table " + index.getTableName(), e);
		}
	}
	
	public static void dropIndex(Index index) {
		if (index == null) {
			throw new SchemaMigrationException("Invalid Index Object");
		}
		
		if (!exists(index)) {
			return;
		}
		
		try {
			Connection connection = Configure.getConnection();
			
			Generator generator = GeneratorFactory.getGenerator(connection);
			
			String query = generator.dropIndex(index);
			
			executeStatement(connection, query);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Unable to drop index " + index.getName() + " from table " + index.getTableName(), e);
		}
	}
	
	public static void addForeignKey(ForeignKey foreignKey) {
		
	}
	
	public static void dropForeignKey(ForeignKey foreignKey) {
		
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
