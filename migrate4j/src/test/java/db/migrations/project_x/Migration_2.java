package db.migrations.project_x;

import com.eroi.migrate.AbstractMigration;
import com.eroi.migrate.Define;

public class Migration_2 extends AbstractMigration {

	public void down() {
		dropColumn("y", ProjectX.X1_TABLE_NAME);
	}

	public void up() {
		addColumn(column("y", Define.DataTypes.VARCHAR, length(12)), ProjectX.X1_TABLE_NAME);
	}

}
