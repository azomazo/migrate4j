package com.sample.migrations;

import java.sql.Types;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class Migration_1 implements Migration {
	
	public static final String TABLE_NAME = "simple_table";
	public static final String COLUMN_ID_NAME = "id";
	public static final String COLUMN_DESC_NAME = "description";
	
	public String getDescription() {
		return "Creates a simple table";
	}

	public void up() {
		Table table = buildTable();
		
		if (!Execute.exists(table)) {
			Execute.createTable(table);
		}
	}

	public void down() {
		Table table = buildTable();
		
		if (Execute.exists(table)) {
			Execute.dropTable(table);
		}
	}

	private Table buildTable() {
		Column[] columns = new Column[2];
		
		columns[0] = Define.column(COLUMN_ID_NAME, Types.INTEGER, -1, true, false, null, true);
		columns[1] = Define.column("COLUMN_DESC_NAME", Types.VARCHAR, 100, false, false, "NA", false);
		
		return Define.table(TABLE_NAME, columns);
	}
	
}
