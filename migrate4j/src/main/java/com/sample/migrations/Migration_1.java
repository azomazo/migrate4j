package com.sample.migrations;

import static com.eroi.migrate.Define.autoincrement;
import static com.eroi.migrate.Define.column;
import static com.eroi.migrate.Define.defaultValue;
import static com.eroi.migrate.Define.length;
import static com.eroi.migrate.Define.notnull;
import static com.eroi.migrate.Define.primarykey;
import static com.eroi.migrate.Define.table;
import static com.eroi.migrate.Define.DataTypes.INTEGER;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.Define.DataTypes;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

/**
 * Adds "simple_table" with "id" and "description" columns
 *
 */
public class Migration_1 implements Migration {
	
	public static final String TABLE_NAME = "simple_table";
	public static final String COLUMN_ID_NAME = "id";
	public static final String COLUMN_DESC_NAME = "description";
	
	public void up() {
		Execute.createTable(buildTable());
	}

	public void down() {
		Execute.dropTable(TABLE_NAME);
	}

	private Table buildTable() {
		//Alternative usage that would work for Java 1.4
		//Column[] columns = new Column[2];
		//columns[0] = new Column(COLUMN_ID_NAME, Types.INTEGER, -1, true, false, null, true);
		//columns[1] = new Column("COLUMN_DESC_NAME", Types.VARCHAR, 100, false, false, "NA", false);
		//return new Table(TABLE_NAME, columns);
		
		Column column1 = column(COLUMN_ID_NAME, INTEGER, primarykey(), notnull(), autoincrement());
		Column column2 = column("COLUMN_DESC_NAME", DataTypes.VARCHAR, length(100), defaultValue("NA"));
		
		return table(TABLE_NAME, column1, column2);
	}
	
}
