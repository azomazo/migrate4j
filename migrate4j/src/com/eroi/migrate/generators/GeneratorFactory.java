package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.SchemaMigrationException;

public class GeneratorFactory {
	private static Log log = LogFactory.getLog(GeneratorFactory.class);
    public static Generator getGenerator(Connection connection) throws SQLException {
        String dbName = connection.getMetaData().getDatabaseProductName();

        if ("H2".equalsIgnoreCase(dbName)) {
            return new H2Generator();
        } else if ("SQL Anywhere".equalsIgnoreCase(dbName)) {
            return new SybaseGenerator();
        } else if ("MySQL".equalsIgnoreCase(dbName)) {
            return new MySQLGenerator();
        } else {
             log.error("No DDLGenerator found for \"" + dbName + "\".  You may need to write your own!");
            throw new SchemaMigrationException("No DDLGenerator found for \"" + dbName + "\".  You may need to write your own!");
        }

    }
}
