package db.migrations;

import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;

public class Migration_9 implements Migration {

	public final static String NEW_TABLE_NAME = "primary_key";
	
	public void down() {
		Execute.renameTable(NEW_TABLE_NAME, Migration_8.TABLE_NAME);
	}

	public void up() {
		Execute.renameTable(Migration_8.TABLE_NAME, NEW_TABLE_NAME);
	}

}
