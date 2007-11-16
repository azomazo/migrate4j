package db.migrations;

import java.sql.Types;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public class Migration_1 implements Migration {

	public static final String TABLE_NAME = "BasicTable";
	
	public void down() {
		Execute.dropTable(getTable());		
	}

	public String getDescription() {
		return "Adds a simple table";
	}

	public void up() {
		Execute.createTable(getTable());
	}

	public static Table getTable() {
		Column[] columns = new Column[2];
		
		columns[0] = Define.column("id", Types.INTEGER, -1, true, false, null, true);
		columns[1] = Define.column("desc", Types.VARCHAR, 50, false, true, null, false);
		
		return new Table(TABLE_NAME, columns);
	}
	
}
