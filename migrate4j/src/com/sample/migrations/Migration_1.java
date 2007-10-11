package com.sample.migrations;

import java.sql.Types;

import com.eroi.migrate.Migration;
import com.eroi.migrate.SchemaBuilder;
import com.eroi.migrate.SchemaElement;

public class Migration_1 implements Migration {
	
	public static final String TABLE_NAME = "simple_table";
	
	public String getDescription() {
		return "Creates a simple table";
	}

	public SchemaElement up() {
		return generateTable();
	}

	public SchemaElement down() {
		return SchemaBuilder.drop(generateTable());
	}

	private SchemaElement generateTable() {
		SchemaElement.Column[] columns = new SchemaElement.Column[2];
		
		columns[0] = (SchemaElement.Column)SchemaBuilder.createPrimaryKeyColumn("id", Types.INTEGER, true);
		columns[1] = (SchemaElement.Column)SchemaBuilder.createColumn("description", Types.VARCHAR, 100, false, "NA", null);
		
		return SchemaBuilder.createTable(TABLE_NAME, columns);
	}
}
