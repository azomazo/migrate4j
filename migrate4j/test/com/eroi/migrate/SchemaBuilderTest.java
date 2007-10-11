package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import com.sample.migrations.Migration_1;

public class SchemaBuilderTest extends TestCase {
	
	protected void setUp() throws Exception {
		super.setUp();
		
		TestHelper.prepareH2Database();
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
		
		TestHelper.resetH2Database();
	}
	
	public void testBuildSchemaElement() throws Exception {
		MigrationRunner runner = TestHelper.getSampleDbMigrationRunner();
		
		Connection connection = null;
		
		try {
			
			connection = runner.getConnection();
			
			assertFalse(tableExists(connection));
			
			SchemaBuilder builder = new SchemaBuilder();
			Migration_1 migration = new Migration_1();
			SchemaElement table = migration.up();
			
			builder.buildSchemaElement(connection, table);
			
			assertTrue(tableExists(connection));
			
			SchemaElement drop = SchemaBuilder.drop(table);
			builder.buildSchemaElement(connection, drop);
			
			assertFalse(tableExists(connection));			
		
		} finally {
			Closer.close(connection);
		}
	}
	
	/* --------------- Helper Methods -------------*/
	
	private boolean tableExists(Connection connection) throws SQLException {
		
		boolean retVal = false;
		
		StringBuffer query = new StringBuffer();
		query.append("select * from \"")
			 .append(Migration_1.TABLE_NAME)
			 .append("\"");
		
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = connection.createStatement();
			try {
				resultSet = statement.executeQuery(query.toString());
				retVal = true;
			} catch (Exception ignored) {
			}
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		return retVal;
	}
}
