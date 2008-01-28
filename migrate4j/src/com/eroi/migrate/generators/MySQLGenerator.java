package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.Configure;
import com.eroi.migrate.misc.Closer;
import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
  * <p>Class MySQLGenerator provides methods for creating statements to 
  * create, alter, or drop tables.</p>
  */
public class MySQLGenerator extends AbstractGenerator {
	private static Log log = LogFactory.getLog(MySQLGenerator.class);
	
	/**
	  * <p>addIndex generates a MySQL alter table statement used to add 
	  * an index to an existing table in a database.</p>
	  * @param index the index to be added
	  * @return String that is the alter table statement
	  */
	public String addIndex(Index index) {
	    if (index == null) {
		log.debug("Null Index located in MySqlGenerator.addIndex(Index)!! Must include a non-null index");
		throw new SchemaMigrationException("Must include a non-null index");
	    }

	    StringBuffer retVal = new StringBuffer();

	    retVal.append("alter table ")
		  .append(wrapName(index.getTableName()))
		  .append(" add ");

	    if (index.isUnique()) {
		retVal.append("unique ");
	    }
	    retVal.append("index ");
	    String indexName = index.getName();
	    if (indexName != null && indexName.length() > 0) {
		retVal.append(wrapName(indexName))
		      .append(" ");
	    }

	    retVal.append("(");

	    String[] columns = index.getColumnNames();
	    String comma = "";
	    for (int x = 0; x < columns.length; x++) {
		retVal.append(comma)
		      .append(wrapName(columns[x]));
		comma = ", ";
	    }

	    retVal.append(");");

	    return retVal.toString();
	}
	
	/**
	  * <p>addPrimaryKey generates a MySQL alter table statement used to 
	  * add a primary key to an existing table in a database.</p>
	  * @param index the primary key to be added
	  * @return String that is the alter table statement
	  */
	public String addPrimaryKey(Index index) {
	    if (index == null) {
		log.debug("Null Index located in MySqlGenerator.addPrimaryKey(Index)!! Must include a non-null index");
		throw new SchemaMigrationException("Must include a non-null index");
	    }

	    StringBuffer retVal = new StringBuffer();

	    retVal.append("alter table ")
		  .append(wrapName(index.getTableName()))
		  .append(" add ");

	    String indexName = index.getName();
	    if (indexName != null && indexName.length() > 0) {
		retVal.append("constraint ")
		      .append(wrapName(indexName));
	    }
	    retVal.append(" PRIMARY KEY(");

	    String[] columns = index.getColumnNames();
	    String comma = "";
	    for (int x = 0; x < columns.length; x++) {
		retVal.append(comma)
		      .append(wrapName(columns[x]));
		comma = ", ";
	    }

	    retVal.append(");");

	    return retVal.toString();
	}
	
	/**
	  * <p>dropIndex generates a MySQL alter table statement used to 
	  * drop an index from an existing table in a database.</p>
	  * @param index the index to be dropped
	  * @return String that is the alter table statement
	  */
	public String dropIndex(Index index) {
	    if (index == null) {
		log.debug("Null Index located in MySqlGenerator.dropIndex(Index)!! Must include a non-null index");
		throw new SchemaMigrationException("Must include a non-null index");
	    }

	    StringBuffer retVal = new StringBuffer();

	    retVal.append("alter table ")
		  .append(wrapName(index.getTableName()))
		  .append(" drop INDEX ");

	    String indexName = index.getName();
	    if (indexName != null && indexName.length() > 0) {
		retVal.append(wrapName(indexName));
	    }
	    retVal.append(";");

	    return retVal.toString();
	}
	
	/**
	  * <p>dropPrimaryKey generates a MySQL alter table statement used to 
	  * drop a primary key from an existing table in a database.</p>
	  * @param index the primary key to be dropped
	  * @return String that is the alter table statement
	  */
	public String dropPrimaryKey(Index index) {
	    if (index == null) {
		log.debug("Null Index located in MySqlGenerator.dropPrimaryKey(Index)!! Must include a non-null index");
		throw new SchemaMigrationException("Must include a non-null index");
	    }

	    StringBuffer retVal = new StringBuffer();

	    retVal.append("alter table ")
		  .append(wrapName(index.getTableName()))
		  .append(" drop PRIMARY KEY;");

	    return retVal.toString();
	}

	/** 
	 * Adds the ability to add table options to a created table.
	 * For example, setting <code>options</code> to "ENGINE=InnoDB"
	 * will create an InnoDB table.
	 * 
	 * @see com.eroi.migrate.generators.Generator#createTableStatement(com.eroi.migrate.schema.Table, java.lang.String)
	 */
	public String createTableStatement(Table table, String options) {
		StringBuffer retVal = new StringBuffer();

		Column[] columns = table.getColumns();

		if (columns == null || columns.length == 0) {
			log.debug("No column located in MySQLGenerator.createTableStatement(Table) !! Table must include at least one column");
		    throw new SchemaMigrationException("Table must include at least one column");
		}

		int numberOfAutoIncrementColumns = GeneratorHelper.countAutoIncrementColumns(columns);
		if (numberOfAutoIncrementColumns > 1) {
			log.debug("Each table must have one and only one auto_increment key.  You included " + numberOfAutoIncrementColumns);
		    throw new SchemaMigrationException("Each table can have at most one auto_increment key.  You included " + numberOfAutoIncrementColumns);
		}
		
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
	
	public String addColumnStatement(Column column, Table table, int position) {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
      * <p>createTableStatememnt generates a MySQL statement used to create a 
      * table in a database if it does not already exists.</p>
      * @param table the table to be created
      * @return String that is the MySQL statement to create the table
      */
    public String createTableStatement(Table table) {
    	return createTableStatement(table, null);	
    }

    /**
      * <p>addComumnStatement generates a MySQL alter table statement that 
      * adds a new column to an existing table as the last column.  This 
      * method calls the addColumnStatement that takes column and table as 
      * parameters and was included to fulfill the contract of the super 
      * class Abstract Generator.</p>
      * @param column the column to be added
      * @param table the table to which to add the column
      * @param afterColumn not used
      * @return String that is the MySQL statement to add the column
      */
    public String addColumnStatement(Column column, Table table, String afterColumn) {
	return addColumnStatement(column, table);
    }

    /**
      * <p>addComumnStatement generates a MySQL alter table statement that 
      * adds a new column to an existing table as the last column.</p>
      * @param column the column to be added
      * @param table the table to which to add the column
      * @return String that is the MySQL statement to add the column
      */
    public String addColumnStatement(Column column, Table table) {

	if (column == null) {
		log.debug("Null Column located in MySqlGenerator.addColumnStatement(Column, Table)!! Must include a non-null column");
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
	    log.debug("Null Table located in MySqlGenerator.addColumnStatement(Column, Table)!! Must include a non-null Table");
	    throw new SchemaMigrationException ("Must provide a table to add the column to");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column, false) + ";");

	return retVal.toString();
    }

    /**
      * <p>addComumnFirstStatement generates a MySQL alter table statement 
      * that adds a new column to an existing table as the first column.</p>
      * @param column the column to be added
      * @param table the table to which to add the column
      * @return String that is the MySQL statement to add the column
      */
    public String addColumnFirstStatement(Column column, Table table) {

	if (column == null) {
		log.debug("Null Column located in MySqlGenerator.addColumnFirstStatement(Column, Table)!! Must include a non-null column");
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
		log.debug("Null Table located in MySqlGenerator.addColumnFirstStatement(Column, Table)!! Must include a non-null Table");
	    throw new SchemaMigrationException ("Must provide a table to add the column too");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column, false))
	      .append(" FIRST;");

	return retVal.toString();
    }

    /**
      * <p>addComumnAfterStatement generates a MySQL alter table statement 
      * that adds a new column to an existing table after the specified 
      * column.</p>
      * @param column the column to be added
      * @param table the table to which to add the column
      * @param afterColumn the column after which the new column is added
      * @return String that is the MySQL statement to add the column
      */
    public String addColumnAfterStatement(Column column, Table table, String afterColumn) {

	if (column == null) {
		log.debug("Null Column located in MySqlGenerator.addColumnAfterStatement(Column, Table,String)!! Must include a non-null column");
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
		log.debug("Null Table located in MySqlGenerator.addColumnAfterStatement(Column, Table,String)!! Must include a non-null Table");
	    throw new SchemaMigrationException ("Must provide a table to add the column too");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column, false))
	      .append(" AFTER ")
	      .append(wrapName(afterColumn))
	      .append(";");

	return retVal.toString();
    }
     
    /**
     * <p>alterEngine generates a MySQL statement that converts a table from 
     * one storage engine to another.</p>
     * @param table the table whose storage engine is to be changed
     * @param engineName the name of the new storage
     * @return String that is the MySQL statement to alter the storage engine
     */
    public String alterEngine(Table table, String engineName) {
	if (table == null) {
	    log.debug("Null table located at MySqlGenerator.alterEngine(Table,String) !! Table should not be null");
	    throw new SchemaMigrationException("Table must not be null");
	}

	StringBuffer retVal = new StringBuffer();
	
	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
 	      .append(" engine = ")
 	      .append(wrapName(engineName))
 	      .append(";");
	
	return retVal.toString();
     }

    /**
      * <p>alterAutoincrement generates a MySQL statement that changes the 
      * value assigned to the auto_increment column for the next entry for 
      * a table.
      * @param table the table whose auto_increment value is to be changed
      * @param value the value of the new auto_increment entry
      * @return String that is the MySQL statement to alter the auto_increment
      * value
      */
    public String alterAutoincrement(Table table, int value) {
	if (table == null) {
           log.debug("Null table located at MySqlGenerator.alterAutoincrement(Table, int) !! Table should not be null");
	    throw new SchemaMigrationException("Table must not be null");
	}

	StringBuffer retVal = new StringBuffer();
	
	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
 	      .append(" auto_increment = ")
 	      .append(value)
 	      .append(";");
	
	return retVal.toString();
    }

    /**
      * <p>dropTableStatement generates a MySQL statement that drops a 
      * table from a database if it exists.</p>
      * @param table the table to be dropped
      * @return String that is the MySQL statement to drop the table
      */
    public String dropTableStatement(Table table) {
	if (table == null) {
	    throw new SchemaMigrationException("Table must not be null");
	}

	StringBuffer retVal = new StringBuffer();
	retVal.append("drop table if exists ")
	      .append(wrapName(table.getTableName()))
	      .append(";");

	return retVal.toString();
    }

    public String getStatement(Statement statement) {
	return null;
    }

    /**
      * <p>makeColumnString generates a String that is used to add the 
      * information about a column to be added to a table.</p>
      * @param column the column to be added
      * @return String containing the column information
      */
    protected String makeColumnString(Column column, boolean suppressPrimaryKey) {
	StringBuffer retVal = new StringBuffer();
	retVal.append(wrapName(column.getColumnName()))
	      .append(" ");

	int type = column.getColumnType();

	retVal.append(GeneratorHelper.getSqlName(type));
	if (GeneratorHelper.needsLength(type)) {
	    retVal.append("(")
		  .append(column.getLength())
		  .append(")");
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
    
    public String getIdentifier() {
    	return "`";
    }

    /** <p>getTableFromDB creates a Table object by querying the database for 
      * information about the columns in a table from the DatabaseMetaData 
      * and the show create table statement.</p>
      * @param connection a Connection to the database
      * @param catalogName a String containing the name of the database
      * @param tableName a String containing the name of the table
      * @return Table object of the table that is in the database
      *
      */
    public Table getTableFromDB(Connection connection, String catalogName, String tableName) {
	String engineName = null;
        Column[] columns = null;
	try {
	    Statement statement = connection.createStatement();
	    ResultSet rs = statement.executeQuery("SHOW CREATE TABLE `" + tableName + "`;");
	    int autoincrementColumn = 0;
	    int autoincrementCount = 0;
	    while (rs.next()) {
			String[] showCreateTableString = rs.getString(2).split("\n");
			for (int i = 0; i < showCreateTableString.length; i++) {
			    if ((showCreateTableString[i].toLowerCase().indexOf("auto_increment") > 0) &&  (showCreateTableString[i].toLowerCase().indexOf("engine") < 0)) {
				autoincrementColumn = i;
				autoincrementCount++;
			    }
	                    if (showCreateTableString[i].toLowerCase().indexOf("engine") > 0) {
	                        String[] engineStringArray = showCreateTableString[i].split("[=\\s]");
	                        for (int i2 = 0; i2 < engineStringArray.length; i2++) {
	                            if (engineStringArray[i2].equalsIgnoreCase("ENGINE")) {
	                                engineName = "ENGINE = " + engineStringArray[i2 + 1];
	                                break;
	                            }
	                        }
	                    }
			}
	    }
	    if (autoincrementCount > 1) {
		throw new SchemaMigrationException("Each table can have at most one auto_increment key.  You included " + autoincrementCount);
	    }
	    DatabaseMetaData dmd = connection.getMetaData();
	    rs= dmd.getPrimaryKeys(catalogName, null, tableName);
	    rs.last();
	    int numberOfPrimaryKeys = rs.getRow();
	    String[] primaryKeyColumnName = new String[numberOfPrimaryKeys];
	    rs.beforeFirst();
	    int k = 0;
	    while (rs.next()) {
		primaryKeyColumnName[k++] = rs.getString(4);
	    }
	    rs = dmd.getColumns(catalogName, null, tableName, null);
	    rs.last();
	    int numberOfColumns = rs.getRow();
	    columns = new Column[numberOfColumns];
	    rs.beforeFirst();
	    int columnNumber = 0;
	    boolean primaryKeyColumn = false;
	    boolean nullableColumn = true;
	    boolean canAutoincrement = false;
	    String columnName = null;
	    k = 0;
	    while (rs.next()) {
		columnName = rs.getString(4);
			for (int j = 0; j < numberOfPrimaryKeys; j++) {
			    if (columnName.equalsIgnoreCase(primaryKeyColumnName[j])) {
				primaryKeyColumn = true;
				break;
			    }
			    primaryKeyColumn = false;
			}
			if (rs.getInt(11) == 0) {
			    nullableColumn = false;
			}
			else {
			    nullableColumn = true;
			}
			if (columnNumber == (autoincrementColumn - 1)) {
			    canAutoincrement = true;
			}
			else {
			    canAutoincrement = false;
			}
			columns[columnNumber] = new Column(columnName, rs.getInt(5), rs.getInt(7), primaryKeyColumn, nullableColumn, rs.getObject(13), canAutoincrement);
			columnNumber++;
	    }
	}
	catch (SQLException ignored) {
           log.error("Error occoured in MySQLGenerator.generateTableFromDb()",ignored);
	}
	return new MySQLTable(tableName, columns, engineName);
    }

	/**
	  * <p>addForeignKey creates a MySQL alter table statement that adds 
	  * a foreign key to an existing table.</p>
	  * @param foreignKey the foreign key to be added
	  * @return String that is the alter table statement
	  */
	public String addForeignKey(ForeignKey foreignKey) {
	    if (foreignKey == null) {
		log.debug("Null ForeignKey located in MySQLGenerator.addForeignKey(ForeignKey)!! Foreign Key must not be null");
		throw new SchemaMigrationException("Must include a non-null foreign key object");
	    }

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
		  .append(");");

	    return retVal.toString();
	}

	/**
	  * <p>dropForeignKey creates a MySQL alter table statement that 
	  * drops a foreign key from an existing table.</p>
	  * @param foreignKey the foreign key to be dropped
	  * @return String that is the alter table statement
	  */
	public String dropForeignKey(ForeignKey foreignKey) {
	    if (foreignKey == null) {
		log.debug("Null ForeignKey located in MySQLGenerator.dropForeignKey(ForeignKey)!! Foreign Key must not be null");
		throw new SchemaMigrationException("Must include a non-null foreign key object");
	    }

	    StringBuffer retVal = new StringBuffer();

	    retVal.append("alter table ")
		  .append(wrapName(foreignKey.getChildTable()))
		  .append(" drop foreign key ");

	    String foreignKeyName = foreignKey.getName();
	    if (foreignKeyName != null && foreignKeyName.length() > 0) {
		retVal.append(wrapName(foreignKeyName));
	    }
	    retVal.append(";");

	    return retVal.toString();
	}

	/**
	  * <p>exists tests for whether or not a foreign key exists.</p>
	  * @param foreignKey the foreign key to be checked
	  * @return boolean indicating the existence of the foreign key
	  */
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
			    if (foreignKey.getParentTable().equals(parentTable) && foreignKey.getParentColumns()[0].equals(parentColumn) && foreignKey.getChildColumns()[0].equals(childColumn)) {
				return true;
			    }
			}
		    }
		}
		finally {
		    Closer.close(resultSet);
		}
		return false;
	    }
	    catch (SQLException exception) {
		log.error("Error occurred on MySQLGenerator.exists(ForeignKey)", exception);
		throw new SchemaMigrationException(exception);
	    }
	}
	
	public static class MySQLTable extends Table {
		
		private String options = null;
		
		public MySQLTable(String tableName, Column[] columns) {
			super(tableName, columns);
		}
		
		public MySQLTable(String tableName, Column[] columns, String options) {
			super(tableName, columns);
			this.options = options;
		}
		
		public String getOptions() {
			return options;
		}
	}
}
