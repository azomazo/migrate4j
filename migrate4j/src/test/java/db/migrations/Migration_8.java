package db.migrations;

import static com.eroi.migrate.Define.autoincrement;
import static com.eroi.migrate.Define.column;
import static com.eroi.migrate.Define.length;
import static com.eroi.migrate.Define.primarykey;
import static com.eroi.migrate.Define.table;
import static com.eroi.migrate.Define.DataTypes.INTEGER;
import static com.eroi.migrate.Define.DataTypes.VARCHAR;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;

public class Migration_8 implements Migration {

	public final static String TABLE_NAME = "prim_key";
	
	public void down() {
		Execute.dropTable(TABLE_NAME);
	}

	public void up() {
		Execute.createTable(
				table(TABLE_NAME, 
						column("id", INTEGER, primarykey(), autoincrement()), 
						column("text", VARCHAR, length(10))));
	
	}

}
