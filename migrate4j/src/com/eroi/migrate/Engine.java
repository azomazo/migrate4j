package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.eroi.migrate.engine.Closer;
import com.eroi.migrate.engine.SchemaMigrationException;

public class Engine {

	private static final Log log = LogFactory.getLog(Engine.class);
	
	public static void migrate() {
		Engine.migrate(Integer.MAX_VALUE);
	}
	
	public static void migrate(int version) {
		
		Class[] migrationClasses = classesToMigrate();
		if (migrationClasses == null || migrationClasses.length <= 0) {
			log.debug("No migration classes match " + Configure.getBaseClassName());
			return;
		}
		
		int currentVersion = -1;
		
		try {
			Connection connection = Configure.getConnection();
			currentVersion = getCurrentVersion(connection);
		} catch (SQLException e) {
			throw new SchemaMigrationException("Failed to get current version from the database", e);
		}

		boolean isUp = isUpMigration(currentVersion, version);
		Class[] classesToMigrate = classesToMigrate();
		classesToMigrate = orderMigrations(classesToMigrate, currentVersion, version);
		
		for (int x = 0 ; x < classesToMigrate.length ; x++) {
			//Execute each migration
			runMigration(classesToMigrate[x], isUp);
		}
	}

	private static void runMigration(Class classToMigrate, boolean isUp) {
		try {
			Migration migration = (Migration)classToMigrate.newInstance();
		
			if (isUp) {
				migration.up();
			} else {
				migration.down();
			}
			
		} catch (InstantiationException e) {
			throw new SchemaMigrationException(e);
		} catch (IllegalAccessException e) {
			throw new SchemaMigrationException(e);
		}
	}
	
	public static int getCurrentVersion(Connection connection) throws SQLException {
		
		//This should run on every JDBC compliant DB . . . I hope
		String query= "select " + Configure.VERSION_FIELD_NAME + " from " + Configure.getVersionTable();
		
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			
			if (resultSet != null && resultSet.next()) {
				return resultSet.getInt(Configure.VERSION_FIELD_NAME);
			} 
			
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		throw new RuntimeException("Couldn't determine current version");
	}

	protected static Class[] classesToMigrate() {
		
		List retVal = new ArrayList();
		String baseName = Configure.getBaseClassName();
		
		int item = Configure.getStartIndex().intValue();
		
		while (true) {
			String classname = baseName + item;
			
			try {
				Class clazz = Class.forName(classname);
				retVal.add(clazz);
			} catch (Exception e) {
				break;
			}
			item++;
		}		
		
		return (Class[])retVal.toArray(new Class[retVal.size()]);
	}
	
	protected static Class[] orderMigrations(Class[] migrationClasses, int currentVersion, int targetVersion) {
		
		List retVal = new ArrayList();
		String baseName = Configure.getBaseClassName();
		boolean goUp = true;
		
		String startClass = baseName + (currentVersion + 1);
		String endClass = baseName + targetVersion;
		
		if (!isUpMigration(currentVersion, targetVersion)) {

			//Going down
			startClass = baseName + (targetVersion + 1);
			endClass = baseName + currentVersion;
			goUp = false;
			
		}
			
		boolean hasStarted = false;
			
		for (int x = 0 ; x < migrationClasses.length ; x++) {
			Class clazz = migrationClasses[x];
			
			if (!hasStarted) {
				
				if (clazz.getName().equals(startClass)) {
					//Just set this to true - the class 
					//will be collected in the next "if" 
					hasStarted = true;
				}
			}
			
			if (hasStarted) {
				
				int index = goUp? retVal.size() : 0;
				retVal.add(index, clazz);
				
				if (clazz.getName().equals(endClass)) {
					//We've already got the class, so
					//just get out
					break;
				}
			}
		}
		
		return (Class[])retVal.toArray(new Class[retVal.size()]);
	}
	
	private static boolean isUpMigration(int currentVersion, int targetVersion) {
		return currentVersion < targetVersion;
	}
}
