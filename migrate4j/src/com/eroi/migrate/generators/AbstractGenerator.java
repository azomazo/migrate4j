package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

public abstract class AbstractGenerator implements Generator {

	public boolean exists(Index index) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesIndexExist(connection, index.getName(), index.getTableName());
		} catch (SQLException exception) {
			throw new SchemaMigrationException(exception);
		}
	}
	
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
	
	public String dropIndex(Index index) {
		
	    if (index == null) {
	        throw new SchemaMigrationException("Must include a non-null index");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("drop index ")
	    	.append(getIdentifier())
	    	.append(index.getName())
	    	.append(getIdentifier());
	    
		return query.toString();
	}
	
	public String getIdentifier() {
		return "\"";
	}

}
