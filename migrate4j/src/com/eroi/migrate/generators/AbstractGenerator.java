package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Table;

public abstract class AbstractGenerator implements Generator {

	public boolean exists(Table table) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesTableExist(connection, table.getTableName());
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		}
	}

	public boolean exists(Column column, Table table) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesColumnExist(connection, column.getColumnName(), table.getTableName());
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		}
	}

	public String dropColumnStatement(Column column, Table table) {
	
	    if (column == null) {
	        throw new SchemaMigrationException("Must include a non-null column");
	    }
	    
	    if (table == null) {
	        throw new SchemaMigrationException ("Must provide a table to drop the column from");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("alter table ")
	    	.append(getIdentifier())
	    	.append(table.getTableName())
	    	.append(getIdentifier())
	    	.append(" drop ")
	    	.append(getIdentifier())
	    	.append(column.getColumnName())
	    	.append(getIdentifier());
	    
		return query.toString();
	}
	
	protected String getIdentifier() {
		return "\"";
	}

}
