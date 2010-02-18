package com.eroi.migrate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;

/**
 * Tells migrate4j where migration classes are located
 * and how to connect to the database.
 *
 */
public class Configure {

	public static final String PROPERTY_CONNECTION_URL = "connection.url";
	public static final String PROPERTY_CONNECTION_DRIVER = "connection.driver";
	public static final String PROPERTY_CONNECTION_USERNAME = "connection.username";
	public static final String PROPERTY_CONNECTION_PASSWORD = "connection.password";
	public static final String PROPERTY_CONNECTION_ARGUMENTS = "connection.arguments";
	public static final String PROPERTY_MIGRATION_PACKAGE_NAME = "migration.package.name";
	public static final String PROPERTY_MIGRATION_CLASSNAME_PREFIX = "migration.classname.prefix";
	public static final String PROPERTY_MIGRATION_SEPARATOR = "migration.separator";
	public static final String PROPERTY_MIGRATION_START_INDEX = "migration.start.index";
	public static final String PROPERTY_DATABASE_VERSION_TABLE = "database.version.table";

	public static final String DEFAULT_PROPERTIES_FILE = "migrate4j.properties";
	public static final String DEFAULT_CLASSNAME_PREFIX = "Migration";
	public static final String DEFAULT_SEPARATOR = "_";
	public static final Integer DEFAULT_START_INDEX = new Integer(1);
	public static final String DEFAULT_VERSION_TABLE = "version";
	
	public static final String VERSION_FIELD_NAME = "version";
	private static Log log = Log.getLog(Configure.class);
	
	/**
	 * Default method to load properties from the
	 * "migrate4j.properties" file, which must be
	 * on the classpath.  The format for this file
	 * is documented in a file named migrate4j.properties.sample
	 * which come with migrate4j.
	 * 
	 */
	public static final void configure() {
		configure(DEFAULT_PROPERTIES_FILE);
	}
	
	/**
	 * Loads properties from a file on the classpath
	 * in the same format as the "migrate4j.properties" 
	 * file, but named <code>propertyFileName</code>
	 * 
	 * @param propertyFileName String file name to find
	 * 		on the classpath
	 */
	public static final void configure(String propertyFileName) {
		Properties properties = null;
		
		if (propertyFileName == null) {
			log.debug("Reading default property File !!");
			propertyFileName = DEFAULT_PROPERTIES_FILE;
		}
		
		try {
	
			InputStream in = Configure.class.getClassLoader().getResourceAsStream(propertyFileName);
			
			if (in != null) {
				
				properties = new Properties();
				properties.load(in);
				configure(properties);
				
			} else {
				throw new RuntimeException ("Could not open an input stream on " + propertyFileName +".  Check that it is in the path " + System.getProperty("java.class.path"));
			}
			
		} catch (IOException e) {
			log.error("Couldn not locate and load property file \"" + propertyFileName + "\"", new IOException());
			throw new RuntimeException("Couldn't locate and load property file \"" + propertyFileName + "\"");
		}
	}
	
	/**
	 * Allows providing specific configuration values programatically via a {@link Properties} object.
	 * 
	 * @param properties a <code>Properties</code> object containing the configuration parameters as name-value pairs. 
	 * Use the <code>PROPERTY_CONNECTION_*</code> or <code>PROPERTY_MIGRATION_*</code> constants defined by this class to fill your property object
	 */
	public static void configure(Properties properties) {
		ConfigStore cfg = getDefaultConfiguration();
		
		// These should be null if not found
		cfg.url = properties.getProperty(PROPERTY_CONNECTION_URL);
		cfg.driver = properties.getProperty(PROPERTY_CONNECTION_DRIVER);
		cfg.username = properties.getProperty(PROPERTY_CONNECTION_USERNAME);
		cfg.password = properties.getProperty(PROPERTY_CONNECTION_PASSWORD);
		cfg.packageName = properties.getProperty(PROPERTY_MIGRATION_PACKAGE_NAME);
		
		// These should be defaults if not found
		setConnectionArguments(properties.getProperty(PROPERTY_CONNECTION_ARGUMENTS));
		setClassprefix(properties.getProperty(PROPERTY_MIGRATION_CLASSNAME_PREFIX));
		setSeparator(properties.getProperty(PROPERTY_MIGRATION_SEPARATOR));
		setVersionTable(properties.getProperty(PROPERTY_DATABASE_VERSION_TABLE));
		
		Integer startIndex = new Integer(-1);
		
		try {
			int i = Integer.parseInt(properties.getProperty(PROPERTY_MIGRATION_START_INDEX));
			startIndex = new Integer(i);
		} catch (Exception ignored) {}
		
		setStartIndex(startIndex);
	}
	
	/**
	 * Uses an existing connection instead of loading 
	 * connection properties from a properties file.
	 * Also requires the package name of the package
	 * where the Migration classes are located.
	 * 
	 * @param connection Connection to connect to the database
	 * @param packageName String name of the package where
	 * 		Migration classes are located
	 */
	public static void configure(Connection connection, String packageName) {
		ConfigStore cfg = getDefaultConfiguration();
		
		cfg.connection = connection;
		cfg.packageName = packageName;
		cfg.ownConnection = false;
	}

	/**
	 * Allows providing specific configuration values programatically.
	 * 
	 * @param url String JDBC url connection string
	 * @param driver String JDBC driver class name
	 * @param username String name of user to use for connecting to the database
	 * @param password  String password for the user
	 * @param connectionArguments  String additional arguments for connecting to the database
	 * @param packageName String package where Migration classes are located
	 * @param classprefix  String beginning of the name of Migration classes
	 * @param separator  String character separating classprefix and numeric value of Migration classes
	 * @param startIndex  Integer first numeric value to look for in Migration class names
	 * @param versionTable  String name of the version table in the database
	 */
	public static void configure(String url, 
								 String driver, 
								 String username, 
								 String password, 
								 String connectionArguments, 
								 String packageName,
								 String classprefix,
								 String separator,
								 Integer startIndex,
								 String versionTable) {

		//Required
		ConfigStore cfg = getDefaultConfiguration();
		
		cfg.url = url;
		cfg.driver = driver;
		cfg.username = username;
		cfg.password = password;
		cfg.packageName = packageName;
		
		//These should be defaults if not found
		setConnectionArguments(connectionArguments);
		setClassprefix(classprefix);
		setStartIndex(startIndex);
		setSeparator(separator);
		setVersionTable(versionTable);
				
	}

	/**
	 * Allows providing specific configuration values programatically.
	 * 
	 * @param url String JDBC url connection string
	 * @param driver String JDBC driver class name
	 * @param username String name of user to use for connecting to the database
	 * @param password  String password for the user
	 * @param packageName String package where Migration classes are located
	 */
	public static void configure(String url, 
			 String driver, 
			 String username, 
			 String password, 
			 String packageName) {
		
		configure(url, 
				  driver, 
				  username, 
				  password, 
				  null, 
				  packageName, 
				  null, 
				  null, 
				  null, 
				  null);
	}
	
	
	public static Connection getConnection() {
		checkConnection();
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.connection;
	}
	
	protected static void close() {
		Configure.cleanConnection();
	}
	
	private static void setConnectionArguments(String connectionArguments) {
		if (connectionArguments == null || connectionArguments.trim().length() == 0) {
			connectionArguments = null;
		}
		
		ConfigStore cfg = getDefaultConfiguration();
		cfg.connectionArguments = connectionArguments;
	}

	private static void setClassprefix(String classprefix) {
		if (classprefix == null || classprefix.trim().length() == 0) {
			classprefix = DEFAULT_CLASSNAME_PREFIX;
		} 
		
		ConfigStore cfg = getDefaultConfiguration();
		cfg.classprefix = classprefix;
	}

	private static void setSeparator(String separator) {
		if (separator == null || separator.trim().length() ==0) {
			separator = DEFAULT_SEPARATOR;
		} 
		
		ConfigStore cfg = getDefaultConfiguration();
		cfg.separator = separator;
	}

	protected static void setStartIndex(Integer startIndex) {
		if (startIndex == null || startIndex.compareTo(new Integer(0)) < 0) {
			startIndex = DEFAULT_START_INDEX;
		}
		
		ConfigStore cfg = getDefaultConfiguration();
		cfg.startIndex = startIndex;
	}

	private static void setVersionTable(String versionTable) {
		if (versionTable == null || versionTable.trim().length() == 0) {
			versionTable = DEFAULT_VERSION_TABLE;
		}
		
		ConfigStore cfg = getDefaultConfiguration();
		cfg.versionTable = versionTable;
	}

	protected static String getUrl() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.url;
	}

	protected static String getDriver() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.driver;
	}

	protected static String getUsername() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.username;
	}

	protected static String getConnectionArguments() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.connectionArguments;
	}

	public static void setPackageName(String packageName) {
	
		ConfigStore cfg = getDefaultConfiguration();
		cfg.packageName = packageName;
	}
	
	public static String getPackageName() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.packageName;
	}

	protected boolean isOwnConnection() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.ownConnection;
	}

	protected static String getClassprefix() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.classprefix;
	}

	protected static String getSeparator() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.separator;
	}

	public static Integer getStartIndex() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.startIndex;
	}

	public static String getVersionTable() {
		
		ConfigStore cfg = getDefaultConfiguration();
		return cfg.versionTable;
	}
	
	public static String getBaseClassName() {
		
		ConfigStore cfg = getDefaultConfiguration();
		String baseName = cfg.packageName + "." + cfg.classprefix + cfg.separator;
		return baseName;
	}
	
	private static void checkConnection() throws SchemaMigrationException {

		ConfigStore cfg = getDefaultConfiguration();

		try {
			if (cfg.connection == null || cfg.connection.isClosed()) {

				Validator.notNull(cfg.driver, "No driver name found!  Make sure you call Configure.configure()");

				cfg.ownConnection = true;
				log.debug("JDBC Driver  "+ cfg.driver);
				Class.forName(cfg.driver);
				cfg.connection = DriverManager.getConnection(cfg.url, cfg.username, cfg.password);
				log.debug("Connection done successfully for "+ cfg.url );
			}
		} catch (ClassNotFoundException e) {
			log.error("Class " + cfg.driver + " not found ", e);
			throw new RuntimeException(e);
			
		} catch (SQLException e) {
			log.error("Unable to check connection " + String.valueOf(cfg.connection));
			throw new SchemaMigrationException(e);
		}
	}
	
	private static void cleanConnection() {
		
		ConfigStore cfg = getDefaultConfiguration();
		if (cfg.ownConnection) {
			Closer.close(cfg.connection);
		}
	}
	
	private final static ThreadLocal<ConfigStore> CFG = new ThreadLocal<ConfigStore>() {
		
		protected ConfigStore initialValue() {
			
			return new ConfigStore();
		}
		
	};

	public synchronized static ConfigStore getDefaultConfiguration() {
		return CFG.get();
	}
	
	protected synchronized static void setDefaultConfiguration(ConfigStore cfg) {
		CFG.set(cfg);
	}
	
}
