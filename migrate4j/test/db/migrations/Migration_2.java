package db.migrations;

import java.sql.Types;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;

/**
 * Adds "status" column to "BasicTable"
 *
 */
public class Migration_2 implements Migration {
	
	public static final String STATUS_COLUMN_NAME = "status";
	
	public void down() {
		Execute.dropColumn(STATUS_COLUMN_NAME, Migration_1.TABLE_NAME);
	}

	public void up() {
		Execute.addColumn(getColumn(), Migration_1.TABLE_NAME);
	}

	public static Column getColumn() {
		return new Column(STATUS_COLUMN_NAME, Types.INTEGER);
	}
}
