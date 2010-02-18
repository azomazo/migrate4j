package db.migrations;

import static com.eroi.migrate.Define.column;
import static com.eroi.migrate.Define.length;
import static com.eroi.migrate.Define.DataTypes.VARCHAR;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;

public class Migration_6 implements Migration {

	public static final String COLUMN_NAME = "randomtext";
	
	public void down() {
		Execute.dropColumn(COLUMN_NAME, Migration_1.TABLE_NAME);
	}

	public void up() {
		Execute.addColumn(column(COLUMN_NAME, VARCHAR, length(20)), Migration_1.TABLE_NAME, Migration_1.DESC_COLUMN_NAME);
	}

}
