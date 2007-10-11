package com.eroi.migrate;

import java.io.InputStream;

import junit.framework.TestCase;

public class MigrationRunnerTest extends TestCase {

	private String testPropertyFile = "migrate4j_test.properties";
	
	public void testPropertiesFileIsFound() {
		InputStream in = getClass().getClassLoader().getResourceAsStream(testPropertyFile);
		assertNotNull("Make sure the \"" + testPropertyFile + "\" file is in your classpath", in);
	}
	
	public void testConstructor_UsingPropertyFile() {
		MigrationRunner migrator = new MigrationRunner(testPropertyFile);
		assertNotNull(migrator);
		
		assertNull(migrator.getConnectionArguments());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", migrator.getUrl());
		assertEquals("com.mysql.jdbc.Driver", migrator.getDriver());
		assertEquals("user", migrator.getUsername());
		assertEquals("com.sample.migrations", migrator.getPackageName());
		assertEquals("DatabaseChange", migrator.getClassprefix());
		assertEquals("-", migrator.getSeparator());
		assertEquals(new Integer(0), migrator.getStartIndex());
		assertEquals("Setup", migrator.getInitClassname());
		assertEquals("tbl_version", migrator.getVersionTable());
	}
	
	public void testConstructor_UsingRequiredArguments() {
		MigrationRunner migrator = new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 "com.sample.migrations");
		
		assertNotNull(migrator);
		
		assertNull(migrator.getConnectionArguments());
		assertNull(migrator.getInitClassname());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", migrator.getUrl());
		assertEquals("com.mysql.jdbc.Driver", migrator.getDriver());
		assertEquals("user", migrator.getUsername());
		assertEquals("com.sample.migrations", migrator.getPackageName());
		assertEquals(MigrationRunner.DEFAULT_CLASSNAME_PREFIX, migrator.getClassprefix());
		assertEquals(MigrationRunner.DEFAULT_SEPARATOR, migrator.getSeparator());
		assertEquals(MigrationRunner.DEFAULT_START_INDEX, migrator.getStartIndex());
		assertEquals(MigrationRunner.DEFAULT_VERSION_TABLE, migrator.getVersionTable());
	}
	
	public void testConstructor_UsingAllArguments() {
		MigrationRunner migrator = new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 null,
										 "com.eroi.migrate.testpackage",
										 "DatabaseChange", 
										 "-",
										 new Integer(0),
										 "tbl_version",
										 "Setup");
		
		assertNotNull(migrator);
		
		assertNull(migrator.getConnectionArguments());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", migrator.getUrl());
		assertEquals("com.mysql.jdbc.Driver", migrator.getDriver());
		assertEquals("user", migrator.getUsername());
		assertEquals("com.eroi.migrate.testpackage", migrator.getPackageName());
		assertEquals("DatabaseChange", migrator.getClassprefix());
		assertEquals("-", migrator.getSeparator());
		assertEquals(new Integer(0), migrator.getStartIndex());
		assertEquals("tbl_version", migrator.getVersionTable());
		assertEquals("Setup", migrator.getInitClassname());
	}
	
	public void testGetMigrationClasses_UsingDefaults() {
		MigrationRunner migrator = new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 null,
										 "com.eroi.migrate.testpackage",
										 null, 
										 null,
										 null,
										 null,
										 null); 
		
		Class[]  list = migrator.getMigrationClasses();
		assertNotNull(list);
		assertEquals(5, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_1", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[2].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[3].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_5", list[4].getName());
		
	}
	
	public void testGetMigrationClasses_UsingCustomSeparator() {
		MigrationRunner migrator = new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
								"com.mysql.jdbc.Driver", 
								"user", 
								"password", 
								null,
								"com.eroi.migrate.testpackage",
								"DataChange", 
								"Version",
								null,
								null,
								null);  
		
		Class[]  list = migrator.getMigrationClasses();
		assertNotNull(list);
		assertEquals(2, list.length);
		assertEquals("com.eroi.migrate.testpackage.DataChangeVersion1", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.DataChangeVersion2", list[1].getName());
		
	}
	
	public void testGetMigrations_UsingClassPrefixThatDoesNotExist() {
		MigrationRunner migrator = new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
								"com.mysql.jdbc.Driver", 
								"user", 
								"password", 
								null,
								"com.eroi.migrate.testpackage",
								"CanNotBeFound", 
								null,
								null,
								null,
								null);  
		
		Class[]  list = migrator.getMigrationClasses();
		assertNotNull(list);
		assertEquals(0, list.length);
	}
	
	public void testOrderMigrations_GoUpFrom3() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[] migrations = migrator.getMigrationClasses();
		
		//Full ugrade with current db at 3
		Class[] list = migrator.orderMigrations(migrations, 3, Integer.MAX_VALUE);
		assertNotNull(list);
		assertEquals(2, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_5", list[1].getName());
		
	}
	
	public void testOrderMigrations_GoUpFrom1To4() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[] migrations = migrator.getMigrationClasses();
		
		Class[] list = migrator.orderMigrations(migrations, 1, 4);
		assertNotNull(list);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoUpFrom2To3() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[]  migrations = migrator.getMigrationClasses();
		
		Class[] list = migrator.orderMigrations(migrations, 2, 3);
		assertEquals(1, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom3() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[]  migrations = migrator.getMigrationClasses();

		Class[] list = migrator.orderMigrations(migrations, 3, 0);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_1", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom4To1() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[]  migrations = migrator.getMigrationClasses();
		
		Class[] list = migrator.orderMigrations(migrations, 4, 1);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom3To2() {

		MigrationRunner migrator = getMigrationRunnerForOrderTest();
		Class[]  migrations = migrator.getMigrationClasses();
		
		Class[] list = migrator.orderMigrations(migrations, 3, 2);
		assertEquals(1, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		
	}
	
	
	/* ----------------- Helper Methods ---------------*/
	
	private MigrationRunner getMigrationRunnerForOrderTest() {
		return new MigrationRunner("jdbc:mysql://localhost:3306/mydb", 
								   "com.mysql.jdbc.Driver",
								   "user",
								   "password",
									"com.eroi.migrate.testpackage"); 
	}
}
