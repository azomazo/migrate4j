package com.eroi.migrate.version;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.ConfigStore;
import com.eroi.migrate.Execute;
import com.eroi.migrate.generators.Generator;
import com.eroi.migrate.generators.GeneratorFactory;
import com.eroi.migrate.misc.Closer;

public class VersionQuery {

	public static void insertVersion(ConfigStore cfg, int version) throws SQLException {
		
		Connection conn = cfg.getConnection();
		Generator g = GeneratorFactory.getGenerator(conn);
		
		String query;

		String versionTableNew = cfg.getFullQualifiedVersionTable();
		if (Execute.tableExists(conn, versionTableNew)) {

	    	String qVersionTab = g.wrapName(versionTableNew);
	    	String qVersionCol = g.wrapName(ConfigStore.VERSION_FIELD_NAME);
	    	String qProjectCol = g.wrapName(ConfigStore.PROJECT_FIELD_NAME);
	    	String projectID = cfg.getProjectID(); 

			// This should run on every JDBC complaint DB . . . I hope
	    	query = String.format(
					"INSERT INTO %s (%s, %s) VALUES ('%s', %d)", 
						qVersionTab,						// INSERT INTO %s
						qProjectCol, 						// (%s, 
						qVersionCol,     					// %s)
						projectID,			       			// VALUES ( '%s', 
						version								// %s);
	    	);
	    	
	    } else {
	    	
	    	throw new IllegalStateException("Found unmigrated version table for configuration " + cfg.toString());

	    }
    	Execute.executeStatement(conn, query);
	}

	public static void updateVersion(ConfigStore cfg, int version) throws SQLException {

		Connection conn = cfg.getConnection();
		Generator g = GeneratorFactory.getGenerator(conn);
		
		String query;
		
		String versionTableNew = cfg.getFullQualifiedVersionTable();
		if (Execute.tableExists(conn, versionTableNew)) {

	    	String qVersionTab = g.wrapName(versionTableNew);
	    	String qVersionCol = g.wrapName(ConfigStore.VERSION_FIELD_NAME);
	    	String qProjectCol = g.wrapName(ConfigStore.PROJECT_FIELD_NAME);
	    	String projectID = cfg.getProjectID(); 
	    	
			// This should run on every JDBC complaint DB . . . I hope
	    	query = String.format(
	    			"UPDATE %s SET %s = %d WHERE %s = '%s'", 
	    				qVersionTab,						// UPDATE %s
	    				qVersionCol, 						// SET %s
	    				version,     						// = %d 
	    				qProjectCol,  						// WHERE %s
	    				projectID							// = '%s'
	    	);
	    	
	    } else {
	    	
	    	throw new IllegalStateException("Found unmigrated version table for configuration " + cfg.toString());
	    }
	    
    	if (0 == Execute.executeStatement(conn, query)) {
    		insertVersion(cfg, version);
    	}
	}
	
	public static int getVersion(ConfigStore cfg) throws SQLException {
		return getVersion(cfg, cfg.getProjectID());
	}

	protected static int getVersion(ConfigStore cfg, String projectID) throws SQLException {

		Connection conn = cfg.getConnection();
		Generator g = GeneratorFactory.getGenerator(conn);
		
		String query;
		
		String versionTableNew = cfg.getFullQualifiedVersionTable();
		if (Execute.tableExists(conn, versionTableNew)) {

	    	String qVersionTab = g.wrapName(versionTableNew);
	    	String qVersionCol = g.wrapName(ConfigStore.VERSION_FIELD_NAME);
	    	String qProjectCol = g.wrapName(ConfigStore.PROJECT_FIELD_NAME);
	    	
			//This should run on every JDBC complaint DB . . . I hope
		    query = String.format(
		    		"SELECT %s FROM %s WHERE %s = '%s'", 
		    			qVersionCol, 						// SELECT %s
		    			qVersionTab, 						// FROM %s
		    			qProjectCol,   						// WHERE %s
		    			projectID							// = '%s'
		    );
		    
	    } else if (VersionMigrator.INSTANCE.isRunning()) {

	    	// version migration is running and no project column so far -> 0
	    	return 0;
	    	
	    } else {
	    	throw new IllegalStateException("Found unmigrated version table for configuration " + cfg.toString());
	    }		
	    
		Statement statement = null;
		ResultSet resultSet = null;
		
		try {
			statement = conn.createStatement();
			resultSet = statement.executeQuery(query);
			
			if (resultSet != null && resultSet.next()) {
				return resultSet.getInt(ConfigStore.VERSION_FIELD_NAME);
			} else {
				// no version stored so far -> must be 0
				return 0;
			}
			
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
		}

	}
}
