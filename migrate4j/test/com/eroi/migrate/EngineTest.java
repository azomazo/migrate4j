package com.eroi.migrate;

import junit.framework.TestCase;

public class EngineTest extends TestCase {
	
	public void testGetMigrationClasses_UsingDefaults() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
										 "com.mysql.jdbc.Driver", 
										 "user", 
										 "password", 
										 null,
										 "com.eroi.migrate.testpackage",
										 null, 
										 null,
										 null,
										 null); 
		
		Class[]  list = Engine.classesToMigrate();
		assertNotNull(list);
		assertEquals(5, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_1", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[2].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[3].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_5", list[4].getName());
		
	}
	
	public void testGetMigrations_UsingClassPrefixThatDoesNotExist() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
								"com.mysql.jdbc.Driver", 
								"user", 
								"password", 
								null,
								"com.eroi.migrate.testpackage",
								"CanNotBeFound", 
								null,
								null,
								null);  
		
		Class[]  list = Engine.classesToMigrate();
		assertNotNull(list);
		assertEquals(0, list.length);
	}
	
	public void testClassesToMigrate_UsingCustomSeparator() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
								"com.mysql.jdbc.Driver", 
								"user", 
								"password", 
								null,
								"com.eroi.migrate.testpackage",
								"DataChange", 
								"Version",
								null,
								null);  
		
		Class[]  list = Engine.classesToMigrate();
		assertNotNull(list);
		assertEquals(2, list.length);
		assertEquals("com.eroi.migrate.testpackage.DataChangeVersion1", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.DataChangeVersion2", list[1].getName());
		
	}
	
	public void testOrderMigrations_GoUpFrom3() {

		Class[] migrations = getClassesToMigrate();
		
		//Full ugrade with current db at 3
		Class[] list = Engine.orderMigrations(migrations, 3, Integer.MAX_VALUE);
		assertNotNull(list);
		assertEquals(2, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_5", list[1].getName());
		
	}
	
	public void testOrderMigrations_GoUpFrom1To4() {
		
		Class[] migrations = getClassesToMigrate();
		
		Class[] list = Engine.orderMigrations(migrations, 1, 4);
		assertNotNull(list);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoUpFrom2To3() {
		
		Class[] migrations = getClassesToMigrate();
		
		Class[] list = Engine.orderMigrations(migrations, 2, 3);
		assertEquals(1, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom3() {

		Class[] migrations = getClassesToMigrate();

		Class[] list = Engine.orderMigrations(migrations, 3, 0);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_1", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom4To1() {

		Class[] migrations = getClassesToMigrate();
		
		Class[] list = Engine.orderMigrations(migrations, 4, 1);
		assertEquals(3, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_4", list[0].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[1].getName());
		assertEquals("com.eroi.migrate.testpackage.Migration_2", list[2].getName());
		
	}
	
	public void testOrderMigrations_GoDownFrom3To2() {

		Class[] migrations = getClassesToMigrate();
		
		Class[] list = Engine.orderMigrations(migrations, 3, 2);
		assertEquals(1, list.length);
		assertEquals("com.eroi.migrate.testpackage.Migration_3", list[0].getName());
		
	}
	
/* ----------------- Helper Methods ---------------*/
	
	private Class[] getClassesToMigrate() {
		Configure.configure("jdbc:mysql://localhost:3306/mydb", 
								   "com.mysql.jdbc.Driver",
								   "user",
								   "password",
									"com.eroi.migrate.testpackage"); 
		
		return Engine.classesToMigrate();
	}
}
