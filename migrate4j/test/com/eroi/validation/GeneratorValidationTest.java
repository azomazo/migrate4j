package com.eroi.validation;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import com.eroi.migrate.Configure;
import com.eroi.migrate.Engine;
import com.eroi.migrate.Execute;
import com.eroi.migrate.generators.GenericGenerator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;

import db.migrations.Migration_1;
import db.migrations.Migration_2;
import db.migrations.Migration_3;
import db.migrations.Migration_4;
import db.migrations.Migration_5;

/**
 * Validates a Generators ability to perform DDL tasks.
 * Validation involves placing a migrat4j properties file
 * (named migrate4j.test.properties) into the test directory
 * and running this TestCase.  Failures indicate which 
 * methods of the Generator are not working.  Passing all
 * tests does not mean the Generator works for all DDL 
 * statements - it just means that it's caught up to
 * the main development of the project.
 *
 */
public class GeneratorValidationTest extends TestCase {
	
	private Connection connection;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		Configure.configure("migrate4j.test.properties");
		
		connection = Configure.getConnection();
		
		if (Engine.getCurrentVersion(connection) > 0) {
			Engine.migrate(0);
		}
		
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testSimpleTableCreation_Version0To1() throws Exception {
		
		assertFalse(Execute.exists(Migration_1.getTable()));
		
		Engine.migrate(1);
		
		assertTrue(Execute.exists(Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
	}
	
	public void testSimpleTableDrop_Version1To0() throws Exception {
		
		Engine.migrate(1);
		assertTrue(Execute.exists(Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		Engine.migrate(0);
		
		assertFalse(Execute.exists(Migration_1.getTable()));
		assertEquals(0, Engine.getCurrentVersion(connection));
	}
	
	public void testAddColumn_Version1To2() throws Exception {
		Engine.migrate(1);
		
		assertFalse(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		Engine.migrate(2);
		
		assertTrue(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(2, Engine.getCurrentVersion(connection));
	}
	
	public void testDropColumn_Version1To2() throws Exception {
		Engine.migrate(2);
		
		assertTrue(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		Engine.migrate(1);
		
		assertFalse(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
	}
	
	public void testAddIndex_Version2To3() throws Exception {
		Engine.migrate(2);
		
		assertFalse(Execute.exists(Migration_3.getIndex()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		Engine.migrate(3);
		
		assertTrue(Execute.exists(Migration_3.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
	}
	
	public void testDropIndex_Version3To2() throws Exception {
		Engine.migrate(3);
		
		assertTrue(Execute.exists(Migration_3.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
		
		Engine.migrate(2);
		
		assertFalse(Execute.exists(Migration_3.getIndex()));
		assertEquals(2, Engine.getCurrentVersion(connection));
	}
	
	public void testAddUniqueIndex_Version3To4() throws Exception {
		Engine.migrate(3);
		
		assertFalse(Execute.exists(Migration_4.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
		
		Engine.migrate(4);
		
		insertDescIntoBasicTable();
		
		try {
			insertDescIntoBasicTable();
			fail("Second addition should have failed!  Index is not unique!");
		} catch (SQLException expected) {
		} catch (Exception exception) {
		}
		
		assertTrue(Execute.exists(Migration_4.getIndex()));
		assertEquals(4, Engine.getCurrentVersion(connection));
	}
	
	public void testDropUniqueIndex_Version4To3() throws Exception {
		Engine.migrate(4);
		
		assertTrue(Execute.exists(Migration_4.getIndex()));
		assertEquals(4, Engine.getCurrentVersion(connection));
		
		Engine.migrate(3);
		
		assertFalse(Execute.exists(Migration_4.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
	}
	
	public void testAddForeignKey_Version4To5() throws Exception {
		Engine.migrate(4);

		assertFalse(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(4, Engine.getCurrentVersion(connection));

		Engine.migrate(5);
		
		assertTrue(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(5, Engine.getCurrentVersion(connection));
	}
	
	public void testDropForeignKey_Version5To4() throws Exception {
		Engine.migrate(5);

		assertTrue(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(5, Engine.getCurrentVersion(connection));
		
		Engine.migrate(4);
		
		assertFalse(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(4, Engine.getCurrentVersion(connection));

	}
	
	/** ------------- Helper Methods ---------------- **/
	private void insertDescIntoBasicTable() throws SQLException {
		Statement s = null;
		
		GenericGenerator generator = (GenericGenerator)GeneratorFactory.getGenerator(connection);
		
		String query = "insert into " 
			+ generator.wrapName(Migration_1.TABLE_NAME) 
			+ " ("
			+ generator.wrapName(Migration_1.DESC_COLUMN_NAME)
			+ ") values ('Desc')";
		
		try {
			s = connection.createStatement();
			s.executeUpdate(query.toString());
			
		} finally {
			Closer.close(s);
		}
		
	}
}
