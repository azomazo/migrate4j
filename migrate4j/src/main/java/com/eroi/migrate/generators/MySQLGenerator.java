package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.Types;

import com.eroi.migrate.misc.Log;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.CascadeRule;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

/**
  * <p>Class MySQLGenerator provides methods for creating statements to 
  * create, alter, or drop tables.</p>
  */
public class MySQLGenerator extends GenericGenerator {
	
	private static Log log = Log.getLog(MySQLGenerator.class);
	
	public MySQLGenerator(Connection aConnection) {
		super(aConnection);
	}

	public String getIdentifier() {
    	return "`";
    }
	
	public String dropIndex(String indexName, String tableName) {
		Validator.notNull(indexName, "Index name can not be null");
		
		StringBuffer retVal = new StringBuffer();
		
		retVal.append("ALTER TABLE ")
			.append(wrapName(tableName))
			.append(" DROP INDEX ")
			.append(wrapName(indexName));
		
		
		return retVal.toString();
	}
	
	public String dropIndex(Index index) {
		Validator.notNull(index, "Index can not be null");
		
	    return dropIndex(index.getName(), index.getTableName());
	}
	
	public String addForeignKey(ForeignKey foreignKey) {
	    Validator.notNull(foreignKey, "Foreign key can not be null");

	    StringBuffer retVal = new StringBuffer();

	    String[] childColumns = wrapStrings(foreignKey.getChildColumns());
	    String[] parentColumns = wrapStrings(foreignKey.getParentColumns());

	    retVal.append("alter table ")
		  .append(wrapName(foreignKey.getChildTable()))
		  .append(" add ");
	    if (foreignKey.getName() != null) {
		retVal.append("constraint ")
		      .append(wrapName(foreignKey.getName()));
	    }
	    retVal.append(" foreign key (")
		  .append(GeneratorHelper.makeStringList(childColumns))
		  .append(") references ")
		  .append(wrapName(foreignKey.getParentTable()))
		  .append(" (")
		  .append(GeneratorHelper.makeStringList(parentColumns))
		  .append(")");
	    
	    if (!CascadeRule.none.equals(foreignKey.getCascadeDeleteRule())) {
	    	retVal.append(" on delete ")
	    		  .append(foreignKey.getCascadeDeleteRule().name());
	    }
	    
	    if (!CascadeRule.none.equals(foreignKey.getCascadeUpdateRule())) {
	    	retVal.append(" on update ")
  		  		  .append(foreignKey.getCascadeUpdateRule().name());
	    }

		retVal .append(";");

	    return retVal.toString();
	}

	
	
	@Override
	public String addColumnStatement(Column column, String tableName, String afterColumn) {
		
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer alter = new StringBuffer();
	    
	    alter.append("ALTER TABLE ")
	         .append(wrapName(tableName))
	         .append(" ADD ")
	         .append(makeColumnString(column, false));
	    
		if (afterColumn != null) {
			alter.append(" after ")
				.append(wrapName(afterColumn));
		}
		
		return alter.toString();
	}
	
	public String createTableStatement(Table table) {
    	return createTableStatement(table, null);	
    }
    
    public String createTableStatement(Table table, String options) {
		StringBuffer retVal = new StringBuffer();

		Validator.notNull(table, "Table can not be null");	
		
		Column[] columns = table.getColumns();

		Validator.notNull(columns, "Columns can not be null");
		Validator.isTrue(columns.length > 0, "At least one column must exist");
		
		int numberOfAutoIncrementColumns = GeneratorHelper.countAutoIncrementColumns(columns);
		
		Validator.isTrue(numberOfAutoIncrementColumns <=1, "Can not have more than one autoincrement key");
		
		boolean hasMultiplePrimaryKeys = GeneratorHelper.countPrimaryKeyColumns(columns) > 1;

		retVal.append("create table if not exists ")
		      .append(wrapName(table.getTableName()))
		      .append(" (");

		try {
		    for (int x = 0; x < columns.length; x++) {
			Column column = (Column)columns[x];

			if (x > 0) {
			    retVal.append(", ");
			}

			retVal.append(makeColumnString(column, hasMultiplePrimaryKeys));
		    }
		} catch (ClassCastException e) {
		    log.error("A table column couldn't be cast to a column: " + e.getMessage());
		    throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
		}

		if (hasMultiplePrimaryKeys) {
		    retVal.append(", PRIMARY KEY(");
		    Column[] primaryKeys = GeneratorHelper.getPrimaryKeyColumns(columns);
		    for (int x = 0; x < primaryKeys.length; x++) {
			Column column = (Column)primaryKeys[x];
			if (x > 0) {
			    retVal.append(", ");
			}
			retVal.append(wrapName(column.getColumnName()));
		    }
		    retVal.append(")");
		}
		
		retVal.append(")");

		if (options != null) {
	            retVal.append(" ").append(options);
	    }
	    retVal.append(";");

	    return retVal.toString();
	}
    
    protected String makeColumnString(Column column, boolean suppressPrimaryKey) {
    	StringBuffer retVal = new StringBuffer();
    	retVal.append(wrapName(column.getColumnName()))
    	      .append(" ");

    	int type = column.getColumnType();
    	
    	String sqlType;
		// HB 09/09: MySQL specific type mapping
		switch (type) {
			case Types.CLOB:
				sqlType = "TEXT";
				break;
			case Types.BLOB:
				// make a decision based on some length data
				if (column.getLength()<=16) {
					sqlType = "BLOB";
				} else 
				if (column.getLength()<=32) {
					sqlType = "MEDIUMBLOB";
				} else {
					sqlType = "LONGBLOB";
				}
				break;
			default:
				sqlType=GeneratorHelper.getSqlName(type);
		} 
		if (sqlType==null) {
			throw new IllegalStateException("Unsupported field type "+type);
		}

		retVal.append(sqlType);
    	
    	if (GeneratorHelper.needsLength(type)) {
    	    retVal.append("(")
    		  .append(column.getLength())
    		  .append(")");
    	}
    	
    	if (GeneratorHelper.acceptsScale(type) || type == Types.NUMERIC) {
    		String end = "";
    		
    		if (column.getPrecision() != null) {
    			retVal.append("(")
    				.append(column.getPrecision());
    			end = ")";
    		}
    		
    		if (column.getScale() != null) {
    			retVal.append(",")
    				.append(column.getScale());
    		}
    		
    		retVal.append(end);
    	}
    	
    	retVal.append(" ");

    	if (!column.isNullable()) {
    	    retVal.append("NOT NULL ");
    	}

    	if (column.isAutoincrement()) {
    	    retVal.append("AUTO_INCREMENT ");
    	}

    	if ((!column.isAutoincrement()) && (column.getDefaultValue() != null)) {
    	    retVal.append("DEFAULT '")
    		  .append(column.getDefaultValue())
    		  .append("' ");
    	}

    	if (!suppressPrimaryKey && column.isPrimaryKey()) {
    	    retVal.append("PRIMARY KEY ");
    	}

    	return retVal.toString().trim();
    }
    
	public String dropForeignKey(String foreignKeyName, String childTable) {
	    Validator.notNull(foreignKeyName, "Foreign key name can not be null");
	    Validator.notNull(childTable, "Child table can not be null");
	    
	    StringBuffer retVal = new StringBuffer();

	    retVal.append("ALTER TABLE ")
		  	.append(wrapName(childTable))
		  	.append(" DROP FOREIGN KEY ")
	    	.append(wrapName(foreignKeyName));

	    return retVal.toString();
	}
	
	@Override
	public String alterColumnStatement(Column definition, String tableName) {
	
		Validator.notNull(definition, "Column definition can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer retVal = new StringBuffer("ALTER TABLE ");
		retVal.append(wrapName(tableName))
			  .append(" MODIFY COLUMN ")
			  .append(makeColumnString(definition, false));
		
		return retVal.toString();
	}

	@Override
	public String renameColumn(String newColumnName, String oldColumnName, String tableName) {

		Validator.notNull(newColumnName, "New column name can not be null");
		Validator.notNull(oldColumnName, "Old column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		Column column = getExistingColumn(oldColumnName, tableName);
		Column newColumn = new Column(newColumnName, column.getColumnType(), column.getLength(), column.isPrimaryKey(), column.isNullable(), column.getDefaultValue(), column.isAutoincrement());
		
		StringBuffer query = new StringBuffer();
		
		query.append("ALTER TABLE ")
			.append(wrapName(tableName))
			.append(" CHANGE COLUMN ")
			.append(wrapName(oldColumnName))
			.append(" ")
			.append(makeColumnString(newColumn, false));
		
		return query.toString();
	}
	
	/**
	 * RENAME TABLE
	 */
	public String renameTableStatement(String tableName, String newName) {
		Validator.notNull(tableName, "Table name must not be null");
		Validator.notNull(newName, "new Table name must not be null");
		
		StringBuffer retVal = new StringBuffer();
		retVal.append("RENAME TABLE ")
			.append(wrapName(tableName))
			.append(" TO ")
			.append(wrapName(newName));
	
		return retVal.toString();
	}
	
}
