package db.migrations;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Index;

public class Migration_4 implements Migration {
	
	private static final String INDEX_NAME = "idx_basictable_desc";
	
	public void down() {
		Execute.dropIndex(INDEX_NAME, Migration_1.TABLE_NAME);
	}

	public String getDescription() {
		return "Adds a uniqe Index to BasicTable";
	}

	public void up() {
		Execute.addIndex(getIndex());
	}

	public static Index getIndex() {
		return Define.uniqueIndex(INDEX_NAME, Migration_1.TABLE_NAME, Migration_1.DESC_COLUMN_NAME);
	}
}
