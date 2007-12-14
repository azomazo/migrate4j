package db.migrations;

import java.sql.Types;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Table;

public class Migration_5 implements Migration {

	public static final String PARENT_TABLE_NAME = "PersonTable";
	public static final String PARENT_COLUMN_KEY = "id";
	public static final String CHILD_TABLE_NAME = "EmployeeTable";
	public static final String CHILD_COLUMN_NAME = "personId";
	
	public void up() {
		Execute.createTable(getPersonTable());
		Execute.createTable(getEmployeeTable());
		Execute.addForeignKey(getForeignKey());
	}
	
	public void down() {
		Execute.dropForeignKey(getForeignKey());
		Execute.dropTable(getEmployeeTable());
		Execute.dropTable(getPersonTable());
	}

	public String getDescription() {
		return "Adds two tables and joins them with a Foreign Key";
	}

	private Table getPersonTable() {
		Column[] columns = new Column[3];
		
		columns[0] = Define.column(PARENT_COLUMN_KEY, Types.INTEGER, -1, true, false, null, true);
		columns[1] = Define.column("name", Types.VARCHAR, 50, false, false, null, false);
		columns[2] = Define.column("email", Types.VARCHAR, 50, false, false, null, false);
		
		return Define.table(PARENT_TABLE_NAME, columns);
	}
	
	private Table getEmployeeTable() {
		Column[] columns = new Column[3];
		
		columns[0] = Define.column("id", Types.INTEGER, -1, true, false, null, true);
		columns[1] = Define.column(CHILD_COLUMN_NAME, Types.INTEGER, -1, false, false, null, false);
		columns[2] = Define.column("hireDate", Types.DATE, -1, false, false, null, false);
		
		return Define.table(CHILD_TABLE_NAME, columns);
	}
	
	public static ForeignKey getForeignKey() {
		return Define.foreignKey(PARENT_TABLE_NAME, PARENT_COLUMN_KEY, CHILD_TABLE_NAME, CHILD_COLUMN_NAME);
	}
}
