package com.eroi.migrate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Applies SQL scripts.  Script paths
 * are passed into the constructor and must
 * be loadable at runtime.
 * 
 * Because the scripts contain DDL, they will
 * likely be platform specific!
 * 
 *
 */
public abstract class AbstractFileBasedMigration implements Migration {

	private static Log log = LogFactory.getLog(AbstractFileBasedMigration.class);
	
	private String upScriptPath;
	private String downScriptPath;
	private String description;
	
	public AbstractFileBasedMigration(String description, String upScriptPath, String downScriptPath){
		this.description = description;
		this.upScriptPath = upScriptPath;
		this.downScriptPath = downScriptPath;
	}

	public String getDescription() {
		return description;
	}

	public SchemaElement up(Connection connection) throws SQLException {
		String script = getFileContents(upScriptPath); 
		
		return new SchemaElement.Procedure(upScriptPath, script, null);
	}
	
	public SchemaElement down(Connection connection) throws SQLException {
		String script = getFileContents(downScriptPath); 
		
		return new SchemaElement.Procedure(upScriptPath, script, null);
	}

	private String getFileContents(String scriptPath) {
		
		StringBuffer retVal = new StringBuffer();
		
		File file = new File(scriptPath);
		
		if (!file.exists()){
			log.warn(scriptPath + " does not exist - no migration work is being done");
			return null;
		}
				
		try {
			BufferedReader buffer = new BufferedReader(new FileReader(file));
			
			String line = null;
			while (( line = buffer.readLine() ) != null ){
				retVal.append(line);
			}
			
		} catch (FileNotFoundException e) {
			log.warn(scriptPath + " could not be loaded - no migration work is being done");
			return null;
		} catch (IOException e) {
			log.warn(scriptPath + " could not be read - no migration work is being done");
			return null;
		}
		
		return retVal.toString();
	}
	

}
