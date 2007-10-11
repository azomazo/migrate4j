package com.eroi.migrate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.generators.GeneratorFactory;


public class SchemaBuilder {

	public void buildSchemaElement(Connection connection, SchemaElement element) throws SQLException {
		
		DDLGenerator generator = GeneratorFactory.getGenerator(connection);
		
		String codeToExcute = null;
				
		if (element instanceof SchemaElement.Table) {
			codeToExcute = generator.getCreateTableStatement((SchemaElement.Table)element);
		} else if (element instanceof SchemaElement.Drop) {
			codeToExcute = generator.getDropStatement(element);
		} else {
			throw new SchemaMigrationException(element.getClass().getName() + " is not supported for migrating yet.");
		}
		
		Statement statement = null;
		
		try {
			statement = connection.createStatement();
			statement.executeUpdate(codeToExcute);
		} catch (SQLException rethrow) {
			rethrow.printStackTrace();
		} finally {
			Closer.close(statement);
		}
		
		
	}
	
	public static SchemaElement createPrimaryKeyColumn(String columnName, 
															   int columnType, 
															   boolean autoincrement) {
		return new SchemaElement.PrimaryKeyColumn(columnName,
														  columnType, 
														  autoincrement);
	}

	public static SchemaElement createPrimaryKeyColumn(String columnName, 
															   int columnType, 
															   int length, 
															   boolean nullable,
															   Object defaultValue, 
															   String afterColumn, 
															   boolean autoincrement) {
		return new SchemaElement.PrimaryKeyColumn(columnName,
														  columnType, 
														  length, 
														  nullable, 
														  defaultValue, 
														  afterColumn,
														  autoincrement);
	}

	public static SchemaElement createColumn(String columnName, int columnType) {
		return new SchemaElement.Column(columnName, columnType);
	}

	public static SchemaElement createColumn(String columnName,
													 int columnType, 
													 int length, 
													 boolean nullable,
													 Object defaultValue, 
													 String afterColumn) {
		return new SchemaElement.Column(columnName, 
												columnType, 
												length,
												nullable, 
												defaultValue, 
												afterColumn);
	}
	
	public static SchemaElement createTable(String tableName, SchemaElement.Column[] columns) {
		return new SchemaElement.Table(tableName, columns);
	}
	
	public static SchemaElement drop(SchemaElement element) {
		return new SchemaElement.Drop(element);
	}
}
