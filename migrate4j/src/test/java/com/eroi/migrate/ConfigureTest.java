package com.eroi.migrate;

import java.io.InputStream;

import junit.framework.TestCase;

public class ConfigureTest extends TestCase {

	private String testPropertyFile = "migrate4j_test.properties";
	
	public void testPropertiesFileIsFound() {
		InputStream in = getClass().getClassLoader().getResourceAsStream(testPropertyFile);
		assertNotNull("Make sure the \"" + testPropertyFile + "\" file is in your classpath", in);
	}
	
	public void testConstructor_UsingPropertyFile() {
		Configure.configure(testPropertyFile);
		
		assertNull(Configure.getConnectionArguments());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", Configure.getUrl());
		assertEquals("com.mysql.jdbc.Driver", Configure.getDriver());
		assertEquals("user", Configure.getUsername());
		assertEquals("com.sample.migrations", Configure.getPackageName());
		assertEquals("DatabaseChange", Configure.getClassprefix());
		assertEquals("-", Configure.getSeparator());
		assertEquals(new Integer(0), Configure.getStartIndex());
		assertEquals("tbl_version", Configure.getVersionTable());
	}
	
	public void testConstructor_UsingRequiredArguments() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 "com.sample.migrations");
		
		assertNull(Configure.getConnectionArguments());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", Configure.getUrl());
		assertEquals("com.mysql.jdbc.Driver", Configure.getDriver());
		assertEquals("user", Configure.getUsername());
		assertEquals("com.sample.migrations", Configure.getPackageName());
		assertEquals(Configure.DEFAULT_CLASSNAME_PREFIX, Configure.getClassprefix());
		assertEquals(Configure.DEFAULT_SEPARATOR, Configure.getSeparator());
		assertEquals(Configure.DEFAULT_START_INDEX, Configure.getStartIndex());
		assertEquals(Configure.DEFAULT_VERSION_TABLE, Configure.getVersionTable());
	}
	
	public void testConstructor_UsingAllArguments() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 null,
										 "com.eroi.migrate.testpackage",
										 "DatabaseChange", 
										 "-",
										 new Integer(0),
										 "tbl_version");
		
		assertNull(Configure.getConnectionArguments());
		
		assertEquals("jdbc:mysql://localhost:3306/mydb", Configure.getUrl());
		assertEquals("com.mysql.jdbc.Driver", Configure.getDriver());
		assertEquals("user", Configure.getUsername());
		assertEquals("com.eroi.migrate.testpackage", Configure.getPackageName());
		assertEquals("DatabaseChange", Configure.getClassprefix());
		assertEquals("-", Configure.getSeparator());
		assertEquals(new Integer(0), Configure.getStartIndex());
		assertEquals("tbl_version", Configure.getVersionTable());
	}
	
}
