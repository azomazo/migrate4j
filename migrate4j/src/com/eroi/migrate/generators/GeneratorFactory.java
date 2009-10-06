package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.misc.Log;

public class GeneratorFactory {
	private static Log log = Log.getLog(GeneratorFactory.class);
	
    public static Generator getGenerator(Connection connection) throws SQLException {
        String dbName = connection.getMetaData().getDatabaseProductName();

        if ("SQL Anywhere".equalsIgnoreCase(dbName)) {
            return new SybaseGenerator(connection);
        } else if ("MySQL".equalsIgnoreCase(dbName)) {
            return new MySQLGenerator(connection);
        } else if ("Apache Derby".equalsIgnoreCase(dbName)) {
        	return new DerbyGenerator(connection);
        } else if ("PostgreSQL".equalsIgnoreCase(dbName)) {
        	return new PostgreSQLGenerator(connection);
        } else {
        	if (!dbName.equals("H2")) {
        		log.warn("No DDLGenerator found for \"" + dbName + "\".  You may need to write your own!  Defaulting to GenericGenerator.");
        	}
            return new GenericGenerator(connection);
        }

    }
}
