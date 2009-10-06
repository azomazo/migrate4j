package db.migrations.project_x;

import com.eroi.migrate.AbstractMigration;
import com.eroi.migrate.Define;

public class Migration_1 extends AbstractMigration {

	public void down() {
		dropTable(ProjectX.X1_TABLE_NAME);
	}

	public void up() {
		createTable(
				table(
					ProjectX.X1_TABLE_NAME, 
					column("id", Define.DataTypes.INTEGER, primarykey(), autoincrement()), 
					column("x", Define.DataTypes.VARCHAR, length(123))));

	}

}
