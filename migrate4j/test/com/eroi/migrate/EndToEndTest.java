package com.eroi.migrate;

import java.sql.Connection;
import java.sql.Types;

import junit.framework.TestCase;

import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;
import com.sample.migrations.Migration_1;

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
		
		//Columns don't really need to be fully defined - we're only checking they exist		
		Column idColumn = Define.column(Migration_1.COLUMN_ID_NAME, Types.INTEGER);
		Column descColumn = Define.column(Migration_1.COLUMN_DESC_NAME, Types.VARCHAR);
		
		Column[] columns = new Column[] { idColumn, descColumn };  
		Table table = Define.table(Migration_1.TABLE_NAME, columns);
		
		try {
			connection = Configure.getConnection();
			
			assertFalse(Execute.exists(table));
			assertFalse(Execute.exists(idColumn, table));
			assertEquals(0, Engine.getCurrentVersion(connection));
			
			Engine.migrate(1);
			
			assertTrue(Execute.exists(table));
			assertTrue(Execute.exists(idColumn, table));
			assertEquals(1, Engine.getCurrentVersion(connection));
			
			Engine.migrate(0);
			
			assertFalse(Execute.exists(table));
			assertFalse(Execute.exists(idColumn, table));
			assertEquals(0, Engine.getCurrentVersion(connection));
			
		} finally {
			Closer.close(connection);
		}
	}
	
}
