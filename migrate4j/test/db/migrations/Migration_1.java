package db.migrations;

import java.sql.Types;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

/**
 * Adds "BasicTable" with "id" and "desc" columns
 *
 */
public class Migration_1 implements Migration {

	public static final String TABLE_NAME = "BasicTable";
	public static final String DESC_COLUMN_NAME = "desc";
	
	public void down() {
		Execute.dropTable(TABLE_NAME);		
	}

	public void up() {
		Execute.createTable(getTable());
	}

	public static Table getTable() {
		Column[] columns = new Column[2];
		
		columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
		columns[1] = new Column(DESC_COLUMN_NAME, Types.VARCHAR, 50, false, true, null, false);
		
		return new Table(TABLE_NAME, columns);
	}
	
}
