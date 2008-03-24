package db.migrations;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Index;

public class Migration_3 implements Migration {
	
	private static final String INDEX_NAME = "idx_basictab_status";
	
	public void down() {
		Execute.dropIndex(INDEX_NAME, Migration_1.TABLE_NAME);
	}

	public String getDescription() {
		return "Adds an Index to BasicTable";
	}

	public void up() {
		Execute.addIndex(getIndex());
	}

	public static Index getIndex() {
		return Define.index(INDEX_NAME, Migration_1.TABLE_NAME, Migration_2.getColumn().getColumnName());
	}
}
