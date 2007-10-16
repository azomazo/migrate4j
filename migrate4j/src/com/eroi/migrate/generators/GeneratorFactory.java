package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.misc.SchemaMigrationException;

public class GeneratorFactory {

	public static Generator getGenerator(Connection connection) throws SQLException {
		String dbName = connection.getMetaData().getDatabaseProductName();
		
		if ("H2".equalsIgnoreCase(dbName)) {
			return new H2Generator();
		} else {
			throw new SchemaMigrationException("No DDLGenerator found for " + dbName + ".  You may need to write your own!");
		}
		
	}
}
