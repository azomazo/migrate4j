package com.eroi.migrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.engine.Closer;
import com.eroi.migrate.engine.SchemaMigrationException;
import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class Execute {

	public static void createTable(Table table){
		
		if (table == null) {
			throw new SchemaMigrationException("Invalid table object");
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
		
	}
	
	public static void dropColumn(Column column, Table table) {
		
	}
	
	public static void statement(String query) {
		
	}
	
	//public static void addIndex(Index index, Table table)
	
	//public static void addForeignKey(ForeignKey key)
	
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
