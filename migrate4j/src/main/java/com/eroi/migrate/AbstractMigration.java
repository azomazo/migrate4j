package com.eroi.migrate;

import java.sql.SQLException;

import com.eroi.migrate.Define.AutoIncrement;
import com.eroi.migrate.Define.DataTypes;
import com.eroi.migrate.Define.DefaultValue;
import com.eroi.migrate.Define.Length;
import com.eroi.migrate.Define.NotNull;
import com.eroi.migrate.Define.Precision;
import com.eroi.migrate.Define.PrimaryKey;
import com.eroi.migrate.Define.Scale;
import com.eroi.migrate.Define.Unicode;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.CascadeRule;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;


public abstract class AbstractMigration implements Migration {

	protected ConfigStore config;

	protected AbstractMigration() {
		this.config = null;
	}
	
	final void setConfigStore(final ConfigStore aConfigStore) {
		this.config = aConfigStore;
	}

	public final ConfigStore getConfiguration() {
		return this.config;
	}
		
	/**
	 * override this method for initialization steps
	 */
	protected void init() {}
	
	// =============================================================================
	//   E X E C U T E 
	// =============================================================================

	/**
	 * Indicates whether an index exists
	 * 
	 * @param index
	 * @return
	 */
	protected final boolean exists(final Index index) {
		return Execute.exists(this.config.getConnection(), index);
	}
	
	/**
	 * Indicates whether an index exists
	 * 
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	protected final boolean indexExists(final String indexName, final String tableName) {
		return Execute.indexExists(this.config.getConnection(), indexName, tableName);
	}
	
	/**
	 * Indicates whether a table exists
	 * 
	 * @param table
	 * @return
	 */
	protected final boolean exists(final Table table) {
		return Execute.exists(this.config.getConnection(), table);
	}
	
	/**
	 * Indicates whether a table exists
	 * 
	 * @param tableName
	 * @return
	 */
	protected final boolean tableExists(final String tableName) {
		return Execute.tableExists(this.config.getConnection(), tableName);
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param column
	 * @param table
	 * @return
	 */
	protected final boolean exists(final Column column, final Table table) {
		return Execute.exists(this.config.getConnection(), table);
		
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param column
	 * @param tableName
	 * @return
	 */
	protected final boolean exists(final Column column, final String tableName) {
		return Execute.exists(this.config.getConnection(), column, tableName);
	}
	
	/**
	 * Indicates whether a table contains a column
	 * 
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	protected final boolean columnExists(final String columnName, final String tableName) {
		return Execute.columnExists(this.config.getConnection(), columnName, tableName);
	}
	
	/**
	 * Indicates whether a foreign key exists
	 * 
	 * @param foreignKeyName
	 * @param childTableName
	 * @return
	 */
	protected final boolean foreignKeyExists(final String foreignKeyName, final String childTableName) {
		return Execute.foreignKeyExists(this.config.getConnection(), foreignKeyName, childTableName);
	}
	
	/**
	 * Indicates whether a foreign key exists
	 * 
	 * @param foreignKey
	 * @return
	 */
	protected final boolean exists(final ForeignKey foreignKey) {
		return Execute.exists(this.config.getConnection(), foreignKey);
	}
	
	/**
	 * Returns true iff the given table contains a primary key
	 * 
	 * @param table
	 * @return
	 */
	protected final boolean hasPrimaryKey(final Table table) {
		return Execute.hasPrimaryKey(this.config.getConnection(), table);
	}

	/**
	 * Returns true iff the given table (defined by its name) contains a primary key
	 * 
	 * @param tableName
	 * @return
	 */
	protected final boolean hasPrimaryKey(final String tableName) {
		return Execute.hasPrimaryKey(this.config.getConnection(), tableName);
	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param column
	 * @param table
	 * @return
	 */
	protected final boolean isPrimaryKey(final Column column, final Table table) {
		return Execute.isPrimaryKey(this.config.getConnection(), column, table);
	}

	/**
	 * Returns true iff the given column is a primary key in the given table
	 * 
	 * @param columnName
	 * @param tableName
	 * @return
	 */
	protected final boolean isPrimaryKey(final String columnName, final String tableName) {
		return Execute.isPrimaryKey(this.config.getConnection(), columnName, tableName);
	}

	
	/**
	 * Create a table
	 * 
	 * @param table
	 */
	protected final void createTable(final Table table) {
		Execute.createTable(this.config.getConnection(), table);
	}
	
	/**
	 * Create a table with database specific options.
	 * This allows, for example, passing an engine type
	 * to MySQL.  While the <code>tableOptions</code>
	 * may be ignored for database products that do not
	 * accept such things, be aware that using this 
	 * argument may make your migrations no longer cross
	 * product compatible.
	 * 
	 * @param table
	 * @param tableOptions
	 */
	protected final void createTable(final Table table, final String tableOptions){
		Execute.createTable(this.config.getConnection(), table, tableOptions);
	}
	
	/**
	 * Drop a table
	 * 
	 * @param tableName
	 */
	protected final void dropTable(final String tableName) {
		Execute.dropTable(this.config.getConnection(), tableName);
	}
	
	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param tableName
	 */
	protected final void addColumn(final Column column, final String tableName) {
		Execute.addColumn(this.config.getConnection(), column, tableName);
	}
	
	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param table
	 * @param afterColumn
	 */
	protected final void addColumn(final Column column, final String table, final String afterColumn) {
		Execute.addColumn(this.config.getConnection(), column, table, afterColumn);
	}
	
	/**
	 * Alter a column
	 * 
	 * @param column
	 * @param tableName
	 */
	protected final void alterColumn(final Column column, final String tableName) {
		Execute.alterColumn(this.config.getConnection(), column, tableName);
	}
	
	/**
	 * Add a column to a table
	 * 
	 * @param column
	 * @param table
	 * @param position
	 */
	protected final void addColumn(final Column column, final String table, final int position) {
		Execute.addColumn(this.config.getConnection(), column, table, position);
	}
	
	/**
	 * Drop a column from a table
	 * 
	 * @param columnName
	 * @param tableName
	 */
	protected final void dropColumn(final String columnName, final String tableName) {
		Execute.dropColumn(this.config.getConnection(), columnName, tableName);
	}
	
	/**
	 * Add an index
	 * 
	 * @param index
	 */
	protected final void addIndex(final Index index) {
		Execute.addIndex(this.config.getConnection(), index);
	}
	
	/**
	 * Drop an index
	 * 
	 * @param indexName
	 * @param tableName
	 */
	protected final void dropIndex(final String indexName, final String tableName) {
		Execute.dropIndex(this.config.getConnection(), indexName, tableName);
	}
	
	/**
	 * Add a foreign key
	 * 
	 * @param foreignKey
	 */
	protected final void addForeignKey(final ForeignKey foreignKey) {
		Execute.addForeignKey(this.config.getConnection(), foreignKey);
	}
	
	/**
	 * Drop a foreign key
	 * 
	 * @param foreignKey
	 */
	protected final void dropForeignKey(final ForeignKey foreignKey) {
		Execute.dropForeignKey(this.config.getConnection(), foreignKey);
	}
	
	/**
	 * Drop a foreign key
	 * 
	 * @param foreignKeyName
	 * @param childTableName
	 */
	protected final void dropForeignKey(final String foreignKeyName, final String childTableName) {
		Execute.dropForeignKey(this.config.getConnection(), foreignKeyName, childTableName);
	}

	/**
	 * Drop a Primary Key from given Table
	 * 
	 * @param tableName
	 */
	protected final void dropPrimaryKey(final String tableName) {
		Execute.dropPrimaryKey(this.config.getConnection(), tableName);
	}

	/**
	 * Drop a Primary Key from given Table
	 * 
	 * @param table
	 */
	protected final void dropPrimaryKey(final Table table) {
		Execute.dropPrimaryKey(this.config.getConnection(), table);
	}
	
	/**
	 * Rename a column
	 * 
	 * @param newColumnName
	 * @param oldColumnName
	 * @param tableName
	 */
	protected final void renameColumn(final String newColumnName, final String oldColumnName, final String tableName) {
		Execute.renameColumn(this.config.getConnection(), newColumnName, oldColumnName, tableName);
	}
		
	protected final int executeStatement(String query) throws SchemaMigrationException {
		try {
			int result = Execute.executeStatement(this.config.getConnection(), query);
			return result;
		} catch (SQLException e) {
			throw new SchemaMigrationException(e);
		}
	}
	
	/**
	 * Rename a table
	 * @param tableName
	 * @param newName
	 */
	protected final void renameTable(final String tableName, final String newName) {
		Execute.renameTable(this.config.getConnection(), tableName, newName);
	}
	
	protected final String wrapName(final String name) {
		return Execute.wrapName(this.config.getConnection(), name);
	}
	
	// =============================================================================
	//   D E F I N E 
	// =============================================================================
	
    /**
     * Represents a column to be added to a schema
     * 
     * @param columnName
     * @param columnType
     * @param columnOption optionally pass any number of ColumnOptions
     */
    protected final Column column(final String columnName, final DataTypes columnType, final Define.ColumnOption<?> ... columnOption) {
    	return Define.column(columnName, columnType, columnOption);
	}

    /**
     * Represents a table to be added to a schema.
     * 
     * @param tableName
     * @param columns
     * @return
     */
    protected final Table table(final String tableName, final Column... columns) {    	
    	return Define.table(tableName, columns);
    }
    
    /**
     * Represents an index to be added to a schema
     * 
     * @param indexName
     * @param tableName
     * @param columnNames
     * @return
     */
    protected final Index index(final String indexName, final String tableName, final String... columnNames) {
    	return Define.index(indexName, tableName, columnNames);
    }
    
    /**
     * Represents a unique index to be added to a schema
     * 
     * @param indexName
     * @param tableName
     * @param columnNames
     * @return
     */
    protected final Index uniqueIndex(final String indexName, final String tableName, final String... columnNames) {
    	return Define.uniqueIndex(indexName, tableName, columnNames);
    }
    
    /**
     * Represents a foreign key to be added to a schema.  Parent
     * table/column are the table which contains the primary key
     * that the child table/column refer to.
     * 
     * @param name
     * @param parentTable
     * @param parentColumn
     * @param childTable
     * @param childColumn
     * @return
     */
    protected final ForeignKey foreignKey(final String name, final String parentTable, final String parentColumn, String childTable, String childColumn) {
    	return Define.foreignKey(name, parentTable, parentColumn, childTable, childColumn);
    }
    
    /**
     * Represents a foreign key to be added to a schema with delete
     * and update cascade rules
     * 
     * @param name
     * @param parentTable
     * @param parentColumn
     * @param childTable
     * @param childColumn
     * @param deleteRule
     * @param updateRule
     * @return
     */
    protected final ForeignKey foreignKey(final String name, final String parentTable, final String parentColumn, final String childTable, final String childColumn, final CascadeRule deleteRule, final CascadeRule updateRule) {
    	return Define.foreignKey(name, parentTable, parentColumn, childTable, childColumn, deleteRule, updateRule);
    }
    
	/**
	 * Allows specifying whether column accepts null
	 * 
	 * @param notnull
	 */
	protected final NotNull notnull(final Boolean notnull) {
		return Define.notnull(notnull);
	}
	
	/**
	 * Indicates column accepts null values;
	 * 
	 */
	protected final NotNull notnull() {
		return Define.notnull();
	}
	
	/**
	 * Allow specifying whether column is auto incrementing
	 * 
	 * @param isAutoincrement
	 * @return
	 */
	protected final AutoIncrement autoincrement(final Boolean isAutoincrement) {
		return Define.autoincrement(isAutoincrement);
	}
	
	/**
	 * Indicates column is auto incrementing
	 * 
	 * @return
	 */
	protected final AutoIncrement autoincrement() {
		return Define.autoincrement();
	}
	
	/**
	 * Indicates whether column should support unicode
	 * 
	 * @param isUnicode
	 * @return
	 */
	protected final Unicode unicode(final Boolean isUnicode) {
		return Define.unicode(isUnicode);
	}
	
	/**
	 * Indicates column should support unicode
	 * 
	 * @return
	 */
	protected final Unicode unicode() {
		return Define.unicode();
	}
	
	/**
	 * Allows specifying whether column is a primary key
	 * 
	 * @param isPrimary
	 * @return
	 */
	protected final PrimaryKey primarykey(final Boolean isPrimary) {
		return Define.primarykey(isPrimary);
	}
	
	/**
	 * Indicates a column is part of a primary key
	 */
	protected final PrimaryKey primarykey() {
		return Define.primarykey();
	}
	
	/**
	 * Allow specifying the length of a column
	 * 
	 * @param len
	 * @return
	 */
	protected final Length length(final Integer len) {
		return Define.length(len);
	}

	/**
	 * Allow specifying the precision of a column
	 * 
	 * @param precision
	 * @return
	 */
	protected final Precision precision(final Integer precision) {
		return Define.precision(precision);
	}

	/**
	 * Allow specifying the scale of a column
	 * 
	 * @param scale
	 * @return
	 */
	protected final Scale scale(final Integer scale) {
		return Define.scale(scale);
	}

	/**
	 * Allows specifying a default value for a column
	 * 
	 * @param obj
	 * @return
	 */
	protected final DefaultValue defaultValue(final Object obj) {
		return Define.defaultValue(obj);
	}
	
	/**
	 * Map {@link Define.DataTypes} constants into <code>AbstractMigration</code>
	 */
	protected final static DataTypes BIGINT = Define.DataTypes.BIGINT; 
	protected final static DataTypes BINARY = Define.DataTypes.BINARY; 
	protected final static DataTypes BIT = Define.DataTypes.BIT; 
	protected final static DataTypes BLOB = Define.DataTypes.BLOB; 
	protected final static DataTypes BOOLEAN = Define.DataTypes.BOOLEAN;
	protected final static DataTypes CHAR = Define.DataTypes.CHAR; 
	protected final static DataTypes CLOB = Define.DataTypes.CLOB;  
	protected final static DataTypes DATE = Define.DataTypes.DATE; 
	protected final static DataTypes DECIMAL = Define.DataTypes.DECIMAL; 
	protected final static DataTypes DOUBLE = Define.DataTypes.DOUBLE; 
	protected final static DataTypes FLOAT = Define.DataTypes.FLOAT; 
	protected final static DataTypes INTEGER = Define.DataTypes.INTEGER; 
	protected final static DataTypes LONGVARBINARY = Define.DataTypes.LONGVARBINARY;  
	protected final static DataTypes LONGVARCHAR = Define.DataTypes.LONGVARCHAR; 
	protected final static DataTypes NUMERIC = Define.DataTypes.NUMERIC; 
	protected final static DataTypes REAL = Define.DataTypes.REAL; 
	protected final static DataTypes SMALLINT = Define.DataTypes.SMALLINT; 
	protected final static DataTypes TIME = Define.DataTypes.TIME; 
	protected final static DataTypes TIMESTAMP = Define.DataTypes.TIMESTAMP; 
	protected final static DataTypes TINYINT = Define.DataTypes.TINYINT; 
	protected final static DataTypes VARBINARY = Define.DataTypes.VARBINARY; 
	protected final static DataTypes VARCHAR = Define.DataTypes.VARCHAR; 
	
}
