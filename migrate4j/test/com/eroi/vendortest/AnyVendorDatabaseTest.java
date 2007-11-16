package com.eroi.vendortest;

import java.sql.Connection;

import junit.framework.TestCase;

import com.eroi.migrate.Configure;
import com.eroi.migrate.Engine;
import com.eroi.migrate.Execute;

import db.migrations.Migration_1;
import db.migrations.Migration_2;

public class AnyVendorDatabaseTest extends TestCase {
	
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
}
