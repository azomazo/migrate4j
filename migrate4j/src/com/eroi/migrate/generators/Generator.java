package com.eroi.migrate.generators;

import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;


public interface Generator {

	public boolean exists(Table table);
	
	public boolean exists(Column column, Table table);
	
	public boolean exists(Index index);
	
	public String createTableStatement(Table table);
	
	public String createTableStatement(Table table, String options);

	public String dropTableStatement(Table table);
	
	public String addColumnStatement(Column column, Table table, String afterColumn);
	
	public String addColumnStatement(Column column, Table table, int position);
	
	public String dropColumnStatement(Column column, Table table);
	
	public String addIndex(Index index);
	
	public String dropIndex(Index index);
}
