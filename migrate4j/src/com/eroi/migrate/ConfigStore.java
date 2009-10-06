package com.eroi.migrate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;

public class ConfigStore {

	public static final String VERSION_FIELD_NAME = Configure.VERSION_FIELD_NAME;
	public static final String PROJECT_FIELD_NAME = "project";
	public static final String PROJECT_ID_UNSPECIFIED = "com.eroi.migrate.unspecified";
	public static final String PROPERTY_MIGRATION_PROJECT_ID = "migration.project.id";
	public static final String PROPERTY_MIGRATION_ESTABLISHED_PROJECT_ID = "migration.established.project.id";
	public static final String VERSION_TABLE_PACKAGE = "com_eroi_migrate_";

	/* Configure with variables */
	protected String url = null;
	protected String driver = null;
	protected String username = null;
	protected String password = null;
	protected String connectionArguments = null;

	protected String packageName = null;
	/* Configure with preconstructed Connection */
	protected Connection connection = null;

	protected boolean ownConnection = false;
	/* Optional configuration parameters */
	protected String classprefix = Configure.DEFAULT_CLASSNAME_PREFIX;
	protected String separator = Configure.DEFAULT_SEPARATOR;
	protected Integer startIndex = Configure.DEFAULT_START_INDEX;
	protected String versionTable = Configure.DEFAULT_VERSION_TABLE;
	protected String projectID = null;
	protected String establishedProjectID = PROJECT_ID_UNSPECIFIED;

	private static Log log = Log.getLog(Configure.class);

	public ConfigStore() {

	}

	/**
	 * Copy Constructor
	 * @param template 
	 */
	public ConfigStore(ConfigStore template) {
		this.url = template.url;
		this.driver = template.driver;
		this.username = template.username;
		this.password = template.password;
		this.connectionArguments = template.connectionArguments;
		this.packageName = template.packageName;

		this.connection = template.connection;
		this.ownConnection = template.ownConnection;

		this.classprefix = template.classprefix;
		this.separator = template.separator;
		this.startIndex = template.startIndex;
		this.versionTable = template.versionTable;
		this.projectID = template.projectID;
		this.establishedProjectID = template.establishedProjectID;
	}

	/**
	 * Uses an existing connection instead of loading 
	 * connection properties from a properties file.
	 * Also requires the package name of the package
	 * where the Migration classes are located 
	 * plus the projectID which identifies the migration (use package-name for example) 
	 * 
	 * @param connection Connection to connect to the database
	 * @param packageName String name of the package where
	 * 		Migration classes are located
	 */
	public ConfigStore(Connection connection, String packageName, String projectID) {

		setConnection(connection);
		setPackageName(packageName);
		setProjectID(projectID);
		setOwnConnection(false);

		// set defaults
		setUrl(null);
		setDriver(null);
		setUsername(null);
		setPassword(null);
		setConnectionArguments(null);
		setClassprefix(null);
		setSeparator(null);
		setStartIndex(null);
		setVersionTable(null);
		setEstablishedProjectID(null);
	}

	public ConfigStore(Properties properties) {

		// These should be null if not found
		setUrl(properties.getProperty(Configure.PROPERTY_CONNECTION_URL));
		setDriver(properties.getProperty(Configure.PROPERTY_CONNECTION_DRIVER));
		setUsername(properties.getProperty(Configure.PROPERTY_CONNECTION_USERNAME));
		setPassword(properties.getProperty(Configure.PROPERTY_CONNECTION_PASSWORD));
		setPackageName(properties.getProperty(Configure.PROPERTY_MIGRATION_PACKAGE_NAME));

		// These should be defaults if not found
		setConnectionArguments(properties.getProperty(Configure.PROPERTY_CONNECTION_ARGUMENTS));
		setClassprefix(properties.getProperty(Configure.PROPERTY_MIGRATION_CLASSNAME_PREFIX));
		setSeparator(properties.getProperty(Configure.PROPERTY_MIGRATION_SEPARATOR));
		setStartIndex(null);
		setVersionTable(properties.getProperty(Configure.PROPERTY_DATABASE_VERSION_TABLE));
		setProjectID(properties.getProperty(PROPERTY_MIGRATION_PROJECT_ID));
		setEstablishedProjectID(properties.getProperty(PROPERTY_MIGRATION_ESTABLISHED_PROJECT_ID));
		
		Integer startIndex = new Integer(-1);

		try {
			int i = Integer.parseInt(properties.getProperty(Configure.PROPERTY_MIGRATION_START_INDEX));
			startIndex = new Integer(i);
		} catch (Exception ignored) {}

		setStartIndex(startIndex);
	}

	public ConfigStore(String propertyFileName) {
		this(_readPropertyFile(propertyFileName));
	}

	/**
	 * Allows providing specific configuration values programatically.
	 * 
	 * @param url String JDBC url connection string
	 * @param driver String JDBC driver class name
	 * @param username String name of user to use for connecting to the database
	 * @param password  String password for the user
	 * @param packageName String package where Migration classes are located
	 * @param projectID String Identifies this migration - use package name for example
	 */
	public ConfigStore(String url, String driver, String username, String password, String packageName, String projectID) {

		setUrl(url);
		setDriver(driver);
		setUsername(username);
		setPassword(password);
		setPackageName(packageName);
		setProjectID(projectID);

		// set defaults
		setConnectionArguments(null);
		setClassprefix(null);
		setSeparator(null);
		setStartIndex(null);
		setVersionTable(null);
		setEstablishedProjectID(null);
	}

	public Object clone() {
		ConfigStore result = new ConfigStore(this);
		return result;
	}

	public String getBaseClassName() {
		
		String baseName = this.packageName + "." + this.classprefix + this.separator;
		return baseName;
	}
	
	public String getClassprefix() {
		return classprefix;
	}

	public void setClassprefix(String classprefix) {
		if (classprefix == null || classprefix.trim().length() == 0) {
			classprefix = Configure.DEFAULT_CLASSNAME_PREFIX;
		} 
		
		this.classprefix = classprefix;
	}

	public Connection getConnection() {

		try {
			if (this.connection == null || this.connection.isClosed()) {

				Validator.notNull(this.driver, "No driver name found!  Make sure you call Configure.configure()");

				this.ownConnection = true;
				log.debug("JDBC Driver  "+ this.driver);
				try {
					Class.forName(this.driver);
					this.connection = DriverManager.getConnection(this.url, this.username, this.password);
				} catch (ClassNotFoundException e) {
					log.error("Class " + this.driver + " not found ", new ClassNotFoundException() );
					throw new RuntimeException(e);
				}
				log.debug("Connection done successfully for "+ this.url );
			}

			return connection;

		} catch (Exception e) {
			throw new SchemaMigrationException(e);
		}
	}
	
	public void setConnection(Connection connection) {
		this.connection = connection;
		this.ownConnection = false;
	}
	

	public String getConnectionArguments() {
		return connectionArguments;
	}
	
	public void setConnectionArguments(String connectionArguments) {
		if (connectionArguments == null || connectionArguments.trim().length() == 0) {
			connectionArguments = null;
		}

		this.connectionArguments = connectionArguments;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		if (driver == null || driver.trim().length() == 0) {
			driver = null;
		}

		this.driver = driver;
	}

	public String getEstablishedProjectID() {
		return establishedProjectID;
	}

	public void setEstablishedProjectID(String establishedProjectID) {
		if (establishedProjectID == null || establishedProjectID.trim().length() == 0) {
			establishedProjectID = PROJECT_ID_UNSPECIFIED;
		}
		this.establishedProjectID = establishedProjectID;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getProjectID() {
		if (this.projectID != null) {
			return this.projectID;
		} else {
			return getEstablishedProjectID();
		}
	}

	public void setProjectID(String projectID) {
		this.projectID = projectID;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		if (separator == null || separator.trim().length() == 0) {
			separator = Configure.DEFAULT_SEPARATOR;
		} 
		
		this.separator = separator;
	}

	public Integer getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(Integer startIndex) {
		if (startIndex == null || startIndex < 0) {
			startIndex = Configure.DEFAULT_START_INDEX;
		}

		this.startIndex = startIndex;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getVersionTable() {
		return versionTable;
	}
	
	public String getFullQualifiedVersionTable() {
		return VERSION_TABLE_PACKAGE + this.versionTable;
	}

	public void setVersionTable(String versionTable) {
		if (versionTable == null || versionTable.trim().length() == 0) {
			versionTable = Configure.DEFAULT_VERSION_TABLE;
		}
		
		this.versionTable = versionTable;
	}

	public boolean isOwnConnection() {
		return ownConnection;
	}

	public void setOwnConnection(boolean ownConnection) {
		this.ownConnection = ownConnection;
	}
	
	@Override
	public String toString() {
		return isOwnConnection()?
			String.format(
				"ConfigStore {Driver: %s, Url: %s, User: %s, Package: %s}", 
					this.driver, 
					this.url, 
					this.username, 
					this.packageName):
			String.format(
				"ConfigStore {Connection to: %s, User: %s, Package: %s}", 
					_getDataBaseName(), 
					this.username, 
					this.packageName);
			
	}
	
	private String _getDataBaseName() {
		try {
			return connection.getCatalog();
		} catch (SQLException e) {
			return e.getMessage();
		}
	}

	private static Properties _readPropertyFile(String propertyFileName) {

		Properties properties = null;

		if (propertyFileName == null) {
			log.debug("Reading default property File !!");
			propertyFileName = Configure.DEFAULT_PROPERTIES_FILE;
		}

		try {
			InputStream in = Configure.class.getClassLoader().getResourceAsStream(propertyFileName);
			if (in != null) {
				properties = new Properties();
				properties.load(in);
				return properties;

			} else {
				throw new RuntimeException ("Could not open an input stream on " + propertyFileName +".  Check that it is in the path " + System.getProperty("java.class.path"));
			}

		} catch (IOException e) {
			log.error("Couldn not locate and load property file \"" + propertyFileName + "\"", new IOException());
			throw new RuntimeException("Couldn't locate and load property file \"" + propertyFileName + "\"");
		}
	}
}
