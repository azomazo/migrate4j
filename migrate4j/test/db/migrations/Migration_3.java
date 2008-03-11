package db.migrations;

import com.eroi.migrate.Define;
import com.eroi.migrate.Execute;
import com.eroi.migrate.Migration;
import com.eroi.migrate.schema.Index;

public class Migration_3 implements Migration {
	
	public void down() {
		Execute.dropIndex(getIndex());
	}

	public String getDescription() {
		return "Adds an Index to BasicTable";
	}

	public void up() {
		Execute.addIndex(getIndex());
	}

	public static Index getIndex() {
		return Define.index("idx_basictab_status", Migration_1.getTable().getTableName(), Migration_2.getColumn().getColumnName());
	}
}
