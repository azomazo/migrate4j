package com.eroi.migrate.generators;

import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;

public class PostgreSQLGenerator extends GenericGenerator {


	/**
	 * <column.name> <column.type>[(<column.length>)]  
	 *      [ DEFAULT default_expr ] [ column_constraint [ ... ] ]
	 * 
	 */
	@Override
	protected String makeColumnString(Column column) {
		StringBuffer retVal = new StringBuffer();

		retVal.append(wrapName(column.getColumnName())).append(" ");

		int type = column.getColumnType();

		if (column.isAutoincrement()) {
			retVal.append("SERIAL ");
		} else {
			retVal.append(GeneratorHelper.getSqlName(type));
		}

		if (GeneratorHelper.needsLength(type)) {
			retVal.append("(").append(column.getLength()).append(")");
		}
		retVal.append(" ");

		if (column.getDefaultValue() != null) {
			retVal.append("DEFAULT '").append(column.getDefaultValue()).append("' ");
		}
		
		if (!column.isNullable()) {
			retVal.append("NOT ");
		}
		retVal.append("NULL ");

		if (column.isPrimaryKey()) {
			retVal.append("PRIMARY KEY ");
		}

		return retVal.toString().trim();
	}

	/**
	 * ALTER TABLE <tableName> ADD <column.name> <column.type>[(<column.length>)] 
	 *      [ column_constraint [ ... ] ]
 	 * 
	 * PostgreSQL neither supports BEFORE nor AFTER, thus <ode>afterColumn</code> is simply ignored
	 * ADD COLUMN uses the same syntax as CREATE TABLE so we can hand over to {@link #makeColumnString(Column)}
	 */
	public String addColumnStatement(Column column, String tableName, String afterColumn) {
		
		Validator.notNull(column, "Column can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer alter = new StringBuffer();
	    
	    alter.append("ALTER TABLE ")
	         .append(wrapName(tableName))
	         .append(" ADD ")
	         .append(makeColumnString(column));
	    
		return alter.toString();
	}
	
	/**
	 * ALTER TABLE <tableName> RENAME <oldColumnName> TO <newColumnName>
	 */
	public String renameColumn(String newColumnName, String oldColumnName, String tableName) {
	
		Validator.notNull(newColumnName, "New column name can not be null");
		Validator.notNull(oldColumnName, "Old column name can not be null");
		Validator.notNull(tableName, "Table name can not be null");
		
		StringBuffer query = new StringBuffer();
		
		query.append("ALTER TABLE ")
			.append(wrapName(tableName))
			.append(" RENAME ")
			.append(wrapName(oldColumnName))
			.append(" TO ")
			.append(wrapName(newColumnName));
		
		return query.toString();
	}	
}
