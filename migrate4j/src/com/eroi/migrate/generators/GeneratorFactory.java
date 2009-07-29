package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GeneratorFactory {
	private static Log log = LogFactory.getLog(GeneratorFactory.class);
	
    public static Generator getGenerator(Connection connection) throws SQLException {
        String dbName = connection.getMetaData().getDatabaseProductName();

        if ("SQL Anywhere".equalsIgnoreCase(dbName)) {
            return new SybaseGenerator();
        } else if ("MySQL".equalsIgnoreCase(dbName)) {
            return new MySQLGenerator();
        } else if ("Apache Derby".equalsIgnoreCase(dbName)) {
        	return new DerbyGenerator();
        } else if ("PostgreSQL".equalsIgnoreCase(dbName)) {
        	return new PostgreSQLGenerator();
        } else {
        	if (!dbName.equals("H2")) {
        		log.warn("No DDLGenerator found for \"" + dbName + "\".  You may need to write your own!  Defaulting to GenericGenerator.");
        	}
            return new GenericGenerator();
        }

    }
}
