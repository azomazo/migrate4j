package com.eroi.validation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;
import junit.textui.TestRunner;

import com.eroi.migrate.Configure;
import com.eroi.migrate.Engine;
import com.eroi.migrate.Execute;
import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GenericGenerator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.generators.PostgreSQLGenerator;
import com.eroi.migrate.misc.Closer;

import db.migrations.Migration_1;
import db.migrations.Migration_2;
import db.migrations.Migration_3;
import db.migrations.Migration_4;
import db.migrations.Migration_5;
import db.migrations.Migration_6;
import db.migrations.Migration_7;

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

	public static final String REPORT_FILE_NAME = "validation_report.log";
	
	private Connection connection;
	
	public static void main(String[] args) {
		File file = new File(REPORT_FILE_NAME);
		file.delete();
		
		try {
			file.createNewFile();
			
			Configure.configure("migrate4j.test.properties");
			Connection conn = Configure.getConnection();
			
			DatabaseMetaData metadata = conn.getMetaData();
			String databaseName = metadata.getDatabaseProductName();
			String databaseVersion = metadata.getDatabaseProductVersion();
			
			String driverName = metadata.getDriverName();
			String driverVersion = metadata.getDriverVersion();
			
			String generator = GeneratorFactory.getGenerator(conn).getClass().getName();
			
			StringBuffer header = new StringBuffer();
			header.append("Test results for ")
				.append(databaseName)
				.append(" version ")
				.append(databaseVersion)
				.append("\n")
				.append("Using driver ")
				.append(driverName)
				.append(" version ")
				.append(driverVersion)
				.append("\n")
				.append("Generator class ")
				.append(generator)
				.append("\n\n");
			
			writeToFile(header.toString());
			
		} catch (IOException e) {
		} catch (SQLException e) {
		}
		
		TestRunner.run(GeneratorValidationTest.class);
		
		try {
			Configure.getConnection().close();
		} catch (SQLException e) {
		}
	}
	
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
		
		writeToFile("\n");
	}
	
	public void testSimpleTableCreation_Version0To1() throws Exception {
		
		writeTestStart("Create table/table exists");
		
		assertFalse(Execute.exists(Migration_1.getTable()));
		
		Engine.migrate(1);
		
		assertTrue(Execute.exists(Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testSimpleTableDrop_Version1To0() throws Exception {
		
		writeTestStart("Drop table");
		
		Engine.migrate(1);
		assertTrue(Execute.exists(Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		Engine.migrate(0);
		
		assertFalse(Execute.exists(Migration_1.getTable()));
		assertEquals(0, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testAddColumn_Version1To2() throws Exception {
		
		writeTestStart("Add column/column exists");
		
		Engine.migrate(1);
		
		assertFalse(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		Engine.migrate(2);
		
		assertTrue(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testDropColumn_Version1To2() throws Exception {
		
		writeTestStart("Drop column");
		
		Engine.migrate(2);
		
		assertTrue(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		Engine.migrate(1);
		
		assertFalse(Execute.exists(Migration_2.getColumn(), Migration_1.getTable()));
		assertEquals(1, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testAddIndex_Version2To3() throws Exception {
		
		writeTestStart("Add index/index exists");
		
		Engine.migrate(2);
		
		assertFalse(Execute.exists(Migration_3.getIndex()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		Engine.migrate(3);
		
		assertTrue(Execute.exists(Migration_3.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testDropIndex_Version3To2() throws Exception {
		
		writeTestStart("Drop index");
		
		Engine.migrate(3);
		
		assertTrue(Execute.exists(Migration_3.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
		
		Engine.migrate(2);
		
		assertFalse(Execute.exists(Migration_3.getIndex()));
		assertEquals(2, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testAddUniqueIndex_Version3To4() throws Exception {
		
		writeTestStart("Add unique index");
		
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
		
		writeTestPass();
	}
	
	public void testDropUniqueIndex_Version4To3() throws Exception {

		writeTestStart("Drop unique index");
		
		Engine.migrate(4);
		
		assertTrue(Execute.exists(Migration_4.getIndex()));
		assertEquals(4, Engine.getCurrentVersion(connection));
		
		Engine.migrate(3);
		
		assertFalse(Execute.exists(Migration_4.getIndex()));
		assertEquals(3, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testAddForeignKey_Version4To5() throws Exception {
		
		writeTestStart("Add foreign key/foreign key exists");
		
		Engine.migrate(4);

		assertFalse(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(4, Engine.getCurrentVersion(connection));

		Engine.migrate(5);
		
		assertTrue(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(5, Engine.getCurrentVersion(connection));
		
		writeTestPass();
	}
	
	public void testDropForeignKey_Version5To4() throws Exception {
		
		writeTestStart("Drop foreign key");
		
		Engine.migrate(5);

		assertTrue(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(5, Engine.getCurrentVersion(connection));
		
		Engine.migrate(4);
		
		assertFalse(Execute.exists(Migration_5.getForeignKey()));
		assertEquals(4, Engine.getCurrentVersion(connection));

		writeTestPass();
	}
	
	public void testAddColumnAfterColumn_Version5To6() throws Exception {
		
		writeTestStart("Add column after column");
		
		Engine.migrate(5);

		assertFalse(Execute.columnExists(Migration_6.COLUMN_NAME, Migration_1.TABLE_NAME));
		assertEquals(5, Engine.getCurrentVersion(connection));
		
		Engine.migrate(6);
		
		assertTrue(Execute.columnExists(Migration_6.COLUMN_NAME, Migration_1.TABLE_NAME));
		assertEquals(6, Engine.getCurrentVersion(connection));

		// Postgres does not support adding columns using BEFORE or AFTER so let's skip it only for Postges 
		if (GeneratorFactory.getGenerator(connection).getClass() != PostgreSQLGenerator.class) {
			assertTrue(checkPlacementOfRandomTextColumn());
		}
			
		
		writeTestPass();
	}

	public void testRenameColumn_Version6To7() throws Exception {
		
		writeTestStart("Rename column");
		
		Engine.migrate(6);
		
		assertTrue(Execute.columnExists(Migration_7.OLD_COLUMN_NAME, Migration_1.TABLE_NAME));
		assertFalse(Execute.columnExists(Migration_7.NEW_COLUMN_NAME, Migration_1.TABLE_NAME));
		
		Engine.migrate(7);
		
		assertFalse(Execute.columnExists(Migration_7.OLD_COLUMN_NAME, Migration_1.TABLE_NAME));
		assertTrue(Execute.columnExists(Migration_7.NEW_COLUMN_NAME, Migration_1.TABLE_NAME));
		
		Engine.migrate(6);
		
		assertTrue(Execute.columnExists(Migration_7.OLD_COLUMN_NAME, Migration_1.TABLE_NAME));
		assertFalse(Execute.columnExists(Migration_7.NEW_COLUMN_NAME, Migration_1.TABLE_NAME));
		
		writeTestPass();
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

	private boolean checkPlacementOfRandomTextColumn() throws SQLException {
		//Make sure column is in correct location
		boolean wasPlacedCorrectly = false;
		
		Generator generator = GeneratorFactory.getGenerator(connection);
		String tableName = generator.wrapName(Migration_1.TABLE_NAME);
		
		String insertQuery = "insert into " + tableName +
			" values (1, 'desc', 'text', 1)";
		String selectQuery = "select * from " + tableName;
		
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = connection.createStatement();
			statement.execute(insertQuery);
			resultSet = statement.executeQuery(selectQuery);
			
			if (resultSet != null && resultSet.next()) {
				String desc = resultSet.getString(2);
				String text = resultSet.getString(3);
				
				wasPlacedCorrectly = "desc".equals(desc) &&
					"text".equals(text);
			}
		} catch (SQLException exception) {
			fail("SQLException encountered while trying to query table columns: " + exception);
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		return wasPlacedCorrectly;
	}
	
	private void writeTestStart(String message) {
		writeToFile(message + ":  ");
	}
	
	private void writeTestPass() {
		writeToFile("PASS");		
	}
	
	private static void writeToFile(String message) {
		
		File file = new File(REPORT_FILE_NAME);
		
		try {
			
			if (!file.exists()) {
				file.createNewFile();
			}
			
			FileWriter writer = new FileWriter(file, true);
			writer.write(message);
			writer.flush();
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
