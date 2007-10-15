package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.engine.Closer;
import com.sample.migrations.Migration_1;

import junit.framework.TestCase;

public class EndToEndTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
		
		TestHelper.prepareH2Database();
		
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		
		TestHelper.resetH2Database();
	}
	
	public void testEndToEndMigration_AddTableToSimpleTable() throws Exception {
		Connection connection = null;
		
		try {
			connection = Configure.getConnection();
			
			assertFalse(hasMigration_1Executed(connection));
			assertEquals(0, Engine.getCurrentVersion(connection));
			
			Engine.migrate();
			
			assertTrue(hasMigration_1Executed(connection));
			//assertEquals(1, Engine.getCurrentVersion(connection));
			
			
		} finally {
			Closer.close(connection);
		}
	}
	
	/* --------------- Helper Methods --------------*/
	
	private boolean hasMigration_1Executed(Connection connection) {
		
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			connection = Configure.getConnection();
			statement = connection.createStatement();
			resultSet = statement.executeQuery("select * from \"" + Migration_1.TABLE_NAME + "\"");
			
			//We'll only get here if the table exists
			return true;
		} catch (SQLException expected){
		} finally {
			Closer.close(resultSet);
			Closer.close(resultSet);
		}
		
		return false;
	}
}
