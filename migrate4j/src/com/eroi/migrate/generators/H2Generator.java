package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Table;

public class H2Generator extends GenericGenerator {
	private static Log log = LogFactory.getLog(H2Generator.class);
	
	public String addColumnStatement(Column column, Table table, int position) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean foreignKeyExists(String foreignKeyName) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public boolean foreignKeyExists(String parentTableName,
			List<String> parentColumnNames, String childTable,
			List<String> childColumnNames) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public String dropIndex(String indexName) {
		// TODO Auto-generated method stub
		return null;
	}

	public String addColumnStatement(Column column, String tableName,
			int position) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String addColumnStatement(Column column, String tableName,
			String afterColumn) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public boolean columnExists(String columnName, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}
		
	public String dropColumnStatement(String columnName, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}


	public String dropForeignKey(ForeignKey foreignKey) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String dropForeignKey(String foreignKeyName) {
		// TODO Auto-generated method stub
		return null;
	}

	public String addColumnStatement(Column column, Table table, String afterColumn) {
		
		Validator.notNull(column, "Column cannot be null");
		Validator.notNull(table, "Table cannot be null");
			    
	    StringBuffer retVal = new StringBuffer();
	    
	    retVal.append("alter table ")
	          .append(wrapName(table.getTableName()))
	          .append(" add ")
	          .append(makeColumnString(column));
	    
	    return retVal.toString();
	    
	}
	
	public String addForeignKey(ForeignKey foreignKey) {

		Validator.notNull(foreignKey, "ForeignKey cannot be null");
		
	    StringBuffer retVal = new StringBuffer();
	    
	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());
	    
	    
	    retVal.append("alter table ")
	    	  .append(wrapName(foreignKey.getChildTable()))
	          .append(" add foreign key (")
	          .append(GeneratorHelper.makeStringList(childColumns))
	          .append(") references ")
	          .append(wrapName(foreignKey.getParentTable()))
	          .append(" (")
	          .append(GeneratorHelper.makeStringList(parentColumns))
	          .append(")");
	    
	    return retVal.toString();
	}

	public boolean exists(ForeignKey foreignKey) {
		
		try {
			Connection connection = Configure.getConnection();
			ResultSet resultSet = null;
			
			try {
			
				DatabaseMetaData databaseMetaData = connection.getMetaData();
			
				resultSet = databaseMetaData.getImportedKeys(null, "", foreignKey.getChildTable());
				
				if (resultSet != null) {
					while (resultSet.next()) {
						String parentTable = resultSet.getString("PKTABLE_NAME");
						String parentColumn = resultSet.getString("PKCOLUMN_NAME");
						String childColumn = resultSet.getString("FKCOLUMN_NAME");
						
						if (foreignKey.getParentTable().equals(parentTable) &&
								foreignKey.getParentColumns()[0].equals(parentColumn) &&
								foreignKey.getChildColumns()[0].equals(childColumn)) {
							return true;
						}
						
					}
				}
			} finally {
				Closer.close(resultSet);
			}
			
			return false;
		} catch (SQLException exception) {
                       log.error("Error occoured in H2Generator.exsists(ForeignKey)",exception);
			throw new SchemaMigrationException(exception);
		}
	}

}
