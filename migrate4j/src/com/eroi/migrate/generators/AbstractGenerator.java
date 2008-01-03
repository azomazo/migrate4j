package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractGenerator implements Generator {
	private static Log log = LogFactory.getLog(AbstractGenerator.class);

	public boolean exists(Index index) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesIndexExist(connection, index.getName(), index.getTableName());
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Index)!!",exception);
			throw new SchemaMigrationException(exception);
		}
	}
	
	public boolean exists(Table table) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesTableExist(connection, table.getTableName());
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Table)!!",exception);
			throw new SchemaMigrationException(exception);
		}
	}

	public boolean exists(Column column, Table table) {
		try {
			Connection connection = Configure.getConnection();
			
			return GeneratorHelper.doesColumnExist(connection, column.getColumnName(), table.getTableName());
		} catch (SQLException exception) {
			log.error("Exception occoured in AbstractGenerator.exists(Column , Table )!!",exception);
			throw new SchemaMigrationException(exception);
		}
	}

	public String dropColumnStatement(Column column, Table table) {
	
	    if (column == null) {
	    	log.debug("Could not locate Column in AbstractGenerator.dropColumnStatement()");
	        throw new SchemaMigrationException("Must include a non-null column");
	    }
	    
	    if (table == null) {
	    	log.debug("Could not locate Table in AbstractGenerator.dropColumnStatement()");
	        throw new SchemaMigrationException ("Must provide a table to drop the column from");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("alter table ")
	    	.append(wrapName(table.getTableName()))
	    	.append(" drop ")
	    	.append(wrapName(column.getColumnName()));
	    
		return query.toString();
	}
	
	public String dropIndex(Index index) {
		
	    if (index == null) {
                  log.debug("Null Index located in AbstractGenerator.dropIndex(Index)");
	        throw new SchemaMigrationException("Must include a non-null index");
	    }
	    
	    StringBuffer query = new StringBuffer();
	    
	    query.append("drop index ")
	    	.append(wrapName(index.getName()));
	    
		return query.toString();
	}
	
	public String dropForeignKey(ForeignKey foreignKey) {
		
		StringBuffer retVal = new StringBuffer();
		
		retVal.append("alter table ")
			.append(wrapName(foreignKey.getChildTable()))
			.append(" drop foreign key ")
			.append(wrapName(foreignKey.getName()));
		
		return retVal.toString();
	}

	public String wrapName(String name) {
		StringBuffer wrap = new StringBuffer();
		
		wrap.append(getIdentifier())
			.append(name)
			.append(getIdentifier());
	
		return wrap.toString();
	}
	
	public String[] wrapStrings(String[] strings) {
		
		String[] wrapped = new String[strings.length];
		
		for (int x = 0 ; x < strings.length ; x++ ) {
			wrapped[x] = wrapName(strings[x]);
		}
		
		return wrapped;
	}
	
	protected String getIdentifier() {
		return "\"";
	}

}
