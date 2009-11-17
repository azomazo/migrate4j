package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;
import com.sample.migrations.Migration_1;

public class SampleMigrationTest extends TestCase {
	
	protected void setUp() throws Exception {
		super.setUp();
		
		TestHelper.prepareH2Database();
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
		
		TestHelper.resetH2Database();
	}
	
	public void testBuildSchemaElement() throws Exception {
		TestHelper.configureSampleDb();
		
		Connection connection = null;
		
		try {
			
			connection = Configure.getConnection();
			
			assertFalse(tableExists(connection));
			
			Migration_1 migration = new Migration_1();
			migration.up();
			
			assertTrue(tableExists(connection));
			
			migration.down();
			
			assertFalse(tableExists(connection));			
		
		} finally {
			Closer.close(connection);
		}
	}
	
	/* --------------- Helper Methods -------------*/
	
	private boolean tableExists(Connection connection) throws SQLException {
		
		boolean retVal = false;
		
		Generator g = GeneratorFactory.getGenerator(connection);
		
		StringBuffer query = new StringBuffer();
		query.append("select * from ")
			 .append(g.wrapName(Migration_1.TABLE_NAME))
			 .append("");
		
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
