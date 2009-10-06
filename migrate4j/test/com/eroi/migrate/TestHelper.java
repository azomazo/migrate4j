package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.misc.Closer;
import com.sample.migrations.Migration_1;

public class TestHelper {

	public static void configureSampleDb() throws SQLException {
		Configure.configure("migrate4j.test.properties");
		Configure.configure(Configure.getConnection(), "com.sample.migrations");
//		Configure.configure("jdbc:h2:~/migrate4j",
//				   			"org.h2.Driver",
//							"sa",
//							"",
//							"com.sample.migrations");
	}
	
	public static void ensureH2DatabaseIsInstalled() throws SQLException {
	
		configureSampleDb();
		
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			connection = Configure.getConnection();
			statement = connection.createStatement();
			
			//This will throw an SQL exception if the table is not there
			resultSet = statement.executeQuery("select " + Configure.VERSION_FIELD_NAME + " from " + Configure.getVersionTable());
			
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
			Closer.close(connection);
		}
	}
	
	public static void resetH2Database() throws SQLException {
		prepareH2Database();
	}
	
	public static void prepareH2Database() throws SQLException {
		configureSampleDb();
		
		String tableName = Configure.getVersionTable();
		String fieldName = Configure.VERSION_FIELD_NAME;
		String sampleTableName = quote(Migration_1.TABLE_NAME);
		
		
		StringBuffer testQuery = new StringBuffer();
		testQuery.append("select ")
				 .append(fieldName)
				 .append(" from ")
				 .append(tableName);
		
		StringBuffer createQuery = new StringBuffer();		
		createQuery.append("create table ")
			 .append(tableName)
			 .append(" (")
			 .append(fieldName)
			 .append(" int not null primary key)");
		
		StringBuffer updateQuery = new StringBuffer();
		updateQuery.append("update ")
				   .append(tableName)
				   .append(" set ")
				   .append(fieldName)
				   .append(" = 0");
		
		StringBuffer insertQuery = new StringBuffer();
		insertQuery.append("insert into ")
				   .append(tableName)
				   .append(" (")
				   .append(fieldName)
				   .append(") values (0)");
		
		StringBuffer dropQuery = new StringBuffer();
		dropQuery.append("drop table ")
				 .append(sampleTableName);
		
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
					
		try {
			connection = Configure.getConnection();
			
			statement = connection.createStatement();
			
			try {
				resultSet = statement.executeQuery(testQuery.toString());
			} catch (SQLException e) {
				statement.executeUpdate(createQuery.toString());
				statement.executeUpdate(insertQuery.toString());
			}
			
			statement.executeUpdate(updateQuery.toString());
			
			try {
				statement.executeUpdate(dropQuery.toString());
			} catch (SQLException ignored){
			}
			
		} finally {
			Closer.close(resultSet);
			Closer.close(statement);
			Closer.close(connection);
		}
	}
	
	private static String quote(String s) {
		return "\"" + s + "\""; 
	}
}
