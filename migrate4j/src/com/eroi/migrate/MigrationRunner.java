package com.eroi.migrate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MigrationRunner {

	public static final String PROPERTY_CONNECTION_URL = "connection.url";
	public static final String PROPERTY_CONNECTION_DRIVER = "connection.driver";
	public static final String PROPERTY_CONNECTION_USERNAME = "connection.username";
	public static final String PROPERTY_CONNECTION_PASSWORD = "connection.password";
	public static final String PROPERTY_CONNECTION_ARGUMENTS = "connection.arguments";
	public static final String PROPERTY_MIGRATION_PACKAGE_NAME = "migration.package.name";
	public static final String PROPERTY_MIGRATION_CLASSNAME_PREFIX = "migration.classname.prefix";
	public static final String PROPERTY_MIGRATION_SEPARATOR = "migration.separator";
	public static final String PROPERTY_MIGRATION_START_INDEX = "migration.start.index";
	public static final String PROPERTY_MIGRATION_INIT_CLASSNAME = "migration.init.classname";
	public static final String PROPERTY_DATABASE_VERSION_TABLE = "database.version.table";
	
	public static final String DEFAULT_CLASSNAME_PREFIX = "Migration";
	public static final String DEFAULT_SEPARATOR = "_";
	public static final Integer DEFAULT_START_INDEX = new Integer(1);
	public static final String DEFAULT_VERSION_TABLE = "version";
	
	public static final String VERSION_FIELD_NAME = "version";
	
	private String url = null;
	private String driver = null;
	private String username = null;
	private String password = null;
	private String connectionArguments = null;
	private String packageName = null;
	
	private Connection connection = null;
	private boolean ownConnection = false;
	
	private String classprefix = DEFAULT_CLASSNAME_PREFIX;
	private String separator = DEFAULT_SEPARATOR;
	private Integer startIndex = DEFAULT_START_INDEX;
	private String versionTable = DEFAULT_VERSION_TABLE;
	private String initClassname = null;
	
	private static final Log log = LogFactory.getLog(MigrationRunner.class);
	
	public MigrationRunner() {
		this("migrate4j.properties");
	}
	
	public MigrationRunner(String propertyFileName) { 
		
		Properties properties = null;
		
		try {
			InputStream in = this.getClass().getClassLoader().getResourceAsStream(propertyFileName);
			if (in != null) {
				properties = new Properties();
				properties.load(in);
				
				//These should be null if not found
				url = properties.getProperty(PROPERTY_CONNECTION_URL);
				driver = properties.getProperty(PROPERTY_CONNECTION_DRIVER);
				username = properties.getProperty(PROPERTY_CONNECTION_USERNAME);
				password = properties.getProperty(PROPERTY_CONNECTION_PASSWORD);
				packageName = properties.getProperty(PROPERTY_MIGRATION_PACKAGE_NAME);
				
				//These should be defaults if not found
				setConnectionArguments(properties.getProperty(PROPERTY_CONNECTION_ARGUMENTS));
				setClassprefix(properties.getProperty(PROPERTY_MIGRATION_CLASSNAME_PREFIX));
				setInitClassname(properties.getProperty(PROPERTY_MIGRATION_INIT_CLASSNAME));
				setSeparator(properties.getProperty(PROPERTY_MIGRATION_SEPARATOR));
				setVersionTable(properties.getProperty(PROPERTY_DATABASE_VERSION_TABLE));
				
				Integer startIndex = new Integer(-1);
				
				try {
					int i = Integer.parseInt(properties.getProperty(PROPERTY_MIGRATION_START_INDEX));
					startIndex = new Integer(i);
				} catch (Exception ignored) {}
				
				setStartIndex(startIndex);
			}
			
		} catch (IOException e) {
			throw new RuntimeException("Couldn't locate and load property file \"" + propertyFileName + "\"");
		}
		
		init();
	}

	public MigrationRunner(Connection connection, String packageName) {
		this.connection = connection;
		this.packageName = packageName;
		
		init();
	}
	
	public MigrationRunner(String url, String driver, String username, String password, String packageName) {
		this(url, driver, username, password, null, packageName, null, null, null, null, null);
		
		init();
	}
	
	public MigrationRunner(String url, 
					String driver, 
					String username, 
					String password, 
					String connectionArguments, 
					String packageName,
					String classprefix,
					String separator,
					Integer startIndex,
					String versionTable,
					String initClassname) {
		
		//Required
		this.url = url;
		this.driver = driver;
		this.username = username;
		this.password = password;
		this.packageName = packageName;
		
		//These should be defaults if not found
		setConnectionArguments(connectionArguments);
		setClassprefix(classprefix);
		setInitClassname(initClassname);
		setStartIndex(startIndex);
		setSeparator(separator);
		setVersionTable(versionTable);
				
		init();
	}
		
	public void migrate() throws SQLException {
		migrate(Integer.MAX_VALUE);
	}
	
	public void migrate(int version) throws SQLException {
		
		Class[] migrationClasses = getMigrationClasses();
		if (migrationClasses == null || migrationClasses.length <= 0) {
			log.debug("No migration classes in package " + packageName + " starting with \"" + classprefix + separator + "\"");
			return;
		}
		
		try {
		
			checkConnection();
			
			int currentVersion = getCurrentVersion();
					
			//TODO:  Init script is currently ignored
			
			SchemaBuilder builder = new SchemaBuilder();
			
			Class[] migrationsToExecute = orderMigrations(migrationClasses, currentVersion, version);
			
			for (int x = 0 ; x < migrationsToExecute.length ; x++) {
				
				runMigrationClass(builder, migrationsToExecute[x]);
			
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		
		} finally {
			cleanConnection();
		}
	}
	
	private void runMigrationClass(SchemaBuilder builder, Class migrationClass) throws SQLException {
		try {
			Migration migration = (Migration)migrationClass.newInstance();
			SchemaElement element = migration.up();
			builder.buildSchemaElement(connection, element);
		} catch (InstantiationException e) {
			throw new SchemaMigrationException("Couldn't instanciate the migration class", e);
		} catch (IllegalAccessException e) {
			throw new SchemaMigrationException("Couldn't instanciate the migration class", e);
		}
	}

	protected int getCurrentVersion() throws SQLException {
		
		//This should run on every JDBC compliant DB . . . I hope
		String query= "select " + VERSION_FIELD_NAME + " from " + versionTable;
		
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(query);
			
			if (resultSet != null && resultSet.next()) {
				return resultSet.getInt(VERSION_FIELD_NAME);
			} 
			
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}
		
		throw new RuntimeException("Couldn't determine current version");
	}

	protected Class[] orderMigrations(Class[] migrationClasses, int currentVersion, int targetVersion) {
		
		List retVal = new ArrayList();
		String baseName = getBaseClassName();
		boolean goUp = true;
		
		String startClass = baseName + (currentVersion + 1);
		String endClass = baseName + targetVersion;
		
		if (currentVersion > targetVersion) {

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
	
 	
	/**
	 * @return List of Class objects
	 */
	protected Class[] getMigrationClasses() {
		
		List retVal = new ArrayList();
		String baseName = getBaseClassName();
		
		int item = startIndex.intValue();
		
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

	private String getBaseClassName() {
		String baseName = packageName + "." + classprefix + separator;
		return baseName;
	}
	
	private void init() {
		if (connection == null) {
			if (url == null || driver == null) {
				throw new RuntimeException("Not enough information to connect to the database");
			}
		}
		
		if (packageName == null) {
			throw new RuntimeException("PackageName is required to migrate");
		}
	}

	protected String getUrl() {
		return url;
	}

	protected String getDriver() {
		return driver;
	}

	protected String getUsername() {
		return username;
	}

	protected String getConnectionArguments() {
		return connectionArguments;
	}

	protected String getPackageName() {
		return packageName;
	}

	protected Connection getConnection() throws SQLException {
		checkConnection();
		
		return connection;
	}

	protected boolean isOwnConnection() {
		return ownConnection;
	}

	protected String getClassprefix() {
		return classprefix;
	}

	protected String getSeparator() {
		return separator;
	}

	protected Integer getStartIndex() {
		return startIndex;
	}

	protected String getVersionTable() {
		return versionTable;
	}
	protected String getInitClassname() {
		return initClassname;
	}
	
	private void setConnectionArguments(String connectionArguments) {
		if (connectionArguments == null || connectionArguments.trim().length() == 0) {
			connectionArguments = null;
		}
		this.connectionArguments = connectionArguments;
	}

	private void setClassprefix(String classprefix) {
		if (classprefix == null || classprefix.trim().length() == 0) {
			classprefix = DEFAULT_CLASSNAME_PREFIX;
		} 
			
		this.classprefix = classprefix;
	}

	private void setSeparator(String separator) {
		if (separator == null || separator.trim().length() ==0) {
			separator = DEFAULT_SEPARATOR;
		} 
		
		this.separator = separator;
	}

	protected void setStartIndex(Integer startIndex) {
		if (startIndex == null || startIndex.compareTo(new Integer(0)) < 0) {
			startIndex = DEFAULT_START_INDEX;
		}
		
		this.startIndex = startIndex;
	}

	private void setVersionTable(String versionTable) {
		if (versionTable == null || versionTable.trim().length() == 0) {
			versionTable = DEFAULT_VERSION_TABLE;
		}
		
		this.versionTable = versionTable;
	}

	private void setInitClassname(String initClassname) {
		if (initClassname == null || initClassname.trim().length() == 0) {
			initClassname = null;
		}
		this.initClassname = initClassname;
	}

	private void checkConnection() throws SQLException {
		if (connection == null || connection.isClosed()) {
			ownConnection = true;
			
			try {
				Class.forName(driver);
				connection = DriverManager.getConnection(url, username, password);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			
		}
	}
	
	private void cleanConnection() {
		if (ownConnection) {
			Closer.close(connection);
		}
	}
}
