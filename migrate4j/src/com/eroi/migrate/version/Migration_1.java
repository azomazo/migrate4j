package com.eroi.migrate.version;

import com.eroi.migrate.AbstractMigration;
import com.eroi.migrate.ConfigStore;

/**
 * Create version table via migration. 
 * We don't have a version for the version table, so we check whether the version table exists
 * 
 */
public class Migration_1 extends AbstractMigration {

	private String versionTableNew;

	public void init() {
		this.versionTableNew = config.getFullQualifiedVersionTable();
	}

	public void up() {
		if (! tableExists(this.versionTableNew)) {
			
			// create new version table with prefixed name and two columns: project (PK), version
			createTable(
					table(	this.versionTableNew, 
							column(ConfigStore.PROJECT_FIELD_NAME, VARCHAR, length(200), primarykey()), 
							column(ConfigStore.VERSION_FIELD_NAME, INTEGER)));
		}
	}

	public void down() {
		dropTable(this.versionTableNew);
	}

}
