package com.sample.migrations;

import java.sql.Types;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class Migration_1 implements Migration {
	
	public static final String TABLE_NAME = "simple_table";
	
	public String getDescription() {
		return "Creates a simple table";
	}

	public void up() {
		Execute.createTable(buildTable());
	}

	public void down() {
		Execute.dropTable(buildTable());
	}

	private Table buildTable() {
		Column[] columns = new Column[2];
		
		columns[0] = Define.column("id", Types.INTEGER, -1, true, false, null, true);
		columns[1] = Define.column("description", Types.VARCHAR, 100, false, false, "NA", false);
		
		return Define.table(TABLE_NAME, columns);
	}
	
}
