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
	
	public void down() {
		Execute.dropColumn(getColumn(), Migration_1.getTable());
	}

	public void up() {
		Execute.addColumn(getColumn(), Migration_1.getTable());
	}

	public static Column getColumn() {
		return new Column("status", Types.INTEGER);
	}
}
