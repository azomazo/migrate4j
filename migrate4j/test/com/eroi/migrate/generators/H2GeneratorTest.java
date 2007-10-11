package com.eroi.migrate.generators;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.eroi.migrate.SchemaBuilder;
import com.eroi.migrate.SchemaElement;

import junit.framework.TestCase;

public class H2GeneratorTest extends TestCase {

	private H2Generator generator;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		generator = new H2Generator();
	}
	
	public void testGetCreateTableStatement() {
		
		String expected = "create table \"sample\" (\"id\" INT NOT NULL PRIMARY KEY,\"desc\" VARCHAR(50) NOT NULL);";
		
		List columns = new ArrayList();
		columns.add(SchemaBuilder.createPrimaryKeyColumn("id", Types.INTEGER, false));
		columns.add(SchemaBuilder.createColumn("desc", Types.VARCHAR, 50, false, null, null));
		
		SchemaElement.Column[] columnArray = 
			(SchemaElement.Column[])columns.toArray(new SchemaElement.Column[columns.size()]);
		
		SchemaElement.Table table = 
			(SchemaElement.Table)SchemaBuilder.createTable("sample", columnArray);

		String statement = generator.getCreateTableStatement(table);
		assertEquals(statement, expected);
	}
	
	public void testGetCreateTableStatement_AutoIncrementingPrimaryKey() {
		
		String expected = "create table \"sample\" (\"id\" INT NOT NULL AUTO_INCREMENT PRIMARY KEY,\"desc\" VARCHAR(50) NOT NULL);";
		
		List columns = new ArrayList();
		columns.add(SchemaBuilder.createPrimaryKeyColumn("id", Types.INTEGER, true));
		columns.add(SchemaBuilder.createColumn("desc", Types.VARCHAR, 50, false, null, null));
		
		SchemaElement.Column[] columnArray = 
			(SchemaElement.Column[])columns.toArray(new SchemaElement.Column[columns.size()]);
		
		SchemaElement.Table table = 
			(SchemaElement.Table)SchemaBuilder.createTable("sample", columnArray);

		String statement = generator.getCreateTableStatement(table);
		assertEquals(statement, expected);
	}
}
