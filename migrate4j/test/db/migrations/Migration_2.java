package db.migrations;

import java.sql.Types;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;

import db.migrations.Migration_1;

public class Migration_2 implements Migration {
	
	public void down() {
		Execute.dropColumn(getColumn(), Migration_1.getTable());
	}

	public String getDescription() {
		return "Adds a column to BasicTable";
	}

	public void up() {
		Execute.addColumn(getColumn(), Migration_1.getTable());
	}

	public static Column getColumn() {
		return Define.column("status", Types.INTEGER);
	}
}
