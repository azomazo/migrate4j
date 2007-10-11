package com.eroi.migrate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.sample.migrations.Migration_1;

public class TestHelper {

	public static MigrationRunner getSampleDbMigrationRunner() {
		return new MigrationRunner("jdbc:h2:~/migrate4j",
				   "org.h2.Driver",
				   "sa",
				   "",
				   "com.sample.migrations");
	}
	
	public static void ensureH2DatabaseIsInstalled() throws SQLException {
		MigrationRunner runner = getSampleDbMigrationRunner();
		Connection connection = null;
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			connection = runner.getConnection();
			statement = connection.createStatement();
			
			//This will throw an SQL exception if the table is not there
			resultSet = statement.executeQuery("select " + MigrationRunner.VERSION_FIELD_NAME + " from " + runner.getVersionTable());
			
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
		MigrationRunner runner = getSampleDbMigrationRunner();
		String tableName = runner.getVersionTable();
		String fieldName = MigrationRunner.VERSION_FIELD_NAME;
		String sampleTableName = "\"" + Migration_1.TABLE_NAME + "\"";
		
		
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
			connection = runner.getConnection();
			
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
}
