package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import com.eroi.migrate.Execute;
import com.eroi.migrate.misc.Validator;
import com.eroi.migrate.schema.Column;

public class PostgreSQLGenerator extends GenericGenerator {


	public PostgreSQLGenerator(Connection aConnection) {
		super(aConnection);
	}

	/**
	 * <column.name> <column.type>[(<column.length>)]  
	 *      [ DEFAULT default_expr ] [ column_constraint [ ... ] ]
	 * 
	 */
	@Override
	protected String makeColumnString(Column column) {
		StringBuilder retVal = new StringBuilder();
		retVal.append(wrapName(column.getColumnName()));
		makeTypeString(retVal,column);
		makeDefaultString(retVal,column);
		makeNotNullString(retVal,column);
		makePrimaryKeyString(retVal,column);
		return retVal.toString();
	}
	
	private String makeTypeString(StringBuilder retVal,Column column) {
		int type = column.getColumnType();
		String sqlType = null;
		// HB 09/09: PSQL specific type mapping
		switch (type) {
			case Types.CLOB:
				sqlType = "TEXT";
				break;
			case Types.BLOB:
				sqlType = "BYTEA";
				break;
			default:
				sqlType=GeneratorHelper.getSqlName(type);
		} 
	
		if (sqlType==null) {
			throw new IllegalStateException("Unsupported field type "+type);
		}

		retVal.append(" ");
		if (column.isAutoincrement()) {
			retVal.append("SERIAL ");
		} else {
			retVal.append(sqlType);
		}

		if (GeneratorHelper.needsLength(type)) {
			retVal.append("(").append(column.getLength()).append(")");
		}
		return retVal.toString();
	}

	
	private void makeDefaultString(StringBuilder retVal, Column column) {
		if (column.getDefaultValue() != null) {
			retVal.append(" DEFAULT '").append(column.getDefaultValue()).append("' ");
		}
	}

	private void makeNotNullString(StringBuilder retVal, Column column) {
		if (!column.isNullable()) {
			retVal.append(" NOT NULL");
		}
	}

	private void makePrimaryKeyString(StringBuilder retVal, Column column) {
		if (column.isPrimaryKey()) {
			retVal.append(" PRIMARY KEY");
		}
	}

	/**
	 * HB 09/09
	 * 
	 * Postgres doesn't use 
	 * ALTER TABLE <tableName> MODIFY COLUMN <existingDefinition.name> 
	 * 		<newDefintion.type> [newDefinition.nullable] [...]
	 * 
	 * but instead we need several statements:
	 * 
	 * ALTER TABLE [ ONLY ] name [ * ] ALTER [ COLUMN ] column TYPE type [ USING expression ]
	 * ALTER TABLE [ ONLY ] name [ * ] ALTER [ COLUMN ] column SET NOT NULL
	 * ALTER TABLE [ ONLY ] name [ * ] ALTER [ COLUMN ] column SET DEFAULT expression
	 */
	@Override
	public void alterColumn(Column column, String tableName) throws SQLException {
		Connection conn = getConnection();
		
		Validator.notNull(column, "Column definition can not be null");
		Validator.notNull(tableName, "Table name can not be null");

		// type
		StringBuilder query = new StringBuilder("ALTER TABLE ");
		query.append(wrapName(tableName))
			  .append(" ALTER COLUMN ")
			  .append(wrapName(column.getColumnName())).append(" TYPE");
		makeTypeString(query, column);
		Execute.executeStatement(conn, query.toString());
		
		// NULLable?
		if (!column.isNullable()) {
			query.setLength(0);
			query.append("ALTER TABLE ")
				 .append(wrapName(tableName))
				 .append(" ALTER COLUMN ")
				 .append(wrapName(column.getColumnName())).append(" SET NOT NULL");
			Execute.executeStatement(conn, query.toString());
		}
		
		// DEFAULT?
		if (column.getDefaultValue()!=null) {
			query.setLength(0);
			query.append("ALTER TABLE ")
				 .append(wrapName(tableName))
				 .append(" ALTER COLUMN ")
				 .append(wrapName(column.getColumnName())).append(" SET");
			makeDefaultString(query, column);
			Execute.executeStatement(conn, query.toString());
		}
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
	
	/**
	 * ALTER TABLE <tableName> DROP CONSTRAINT <tableName>_pkey CASCADE;
	 */
	public String dropPrimaryKey(String tableName) {
		Validator.notNull(tableName, "Table name can not be null");

		String wrappedTabName = wrapName(tableName);
		String wrappedConstr = wrapName(tableName + "_pkey");
		
		String result = String.format("ALTER TABLE %s DROP CONSTRAINT %s CASCADE", wrappedTabName, wrappedConstr);
		return result;
	}
}
