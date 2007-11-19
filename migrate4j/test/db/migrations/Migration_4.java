package db.migrations;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Index;

public class Migration_4 implements Migration {
	
	public void down() {
		Execute.dropIndex(getIndex());
	}

	public String getDescription() {
		return "Adds a uniqe Index to BasicTable";
	}

	public void up() {
		Execute.addIndex(getIndex());
	}

	public static Index getIndex() {
		return Define.uniqueIndex(Migration_1.getTable().getTableName(), Migration_1.DESC_COLUMN_NAME);
	}
}
