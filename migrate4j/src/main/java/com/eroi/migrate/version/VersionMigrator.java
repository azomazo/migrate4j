package com.eroi.migrate.version;

import com.eroi.migrate.ConfigStore;
import com.eroi.migrate.Engine;

public class VersionMigrator {

	public final static VersionMigrator INSTANCE = new VersionMigrator();
	
	private boolean isRunning;
	
	private VersionMigrator() {
		this.isRunning = false;
	}

	public void runMigration(ConfigStore configTemplate) {
		this.isRunning = true;

		ConfigStore myConfig = new ConfigStore(configTemplate);
		String myPackage = this.getClass().getPackage().getName();
		
		myConfig.setPackageName(myPackage);
		myConfig.setProjectID(myPackage);
		
		Engine.migrate(myConfig);
	}
	
	public void endMigration() {
		this.isRunning = false;
	}
	
	public boolean isRunning() {
		return this.isRunning;
	}
	
}
