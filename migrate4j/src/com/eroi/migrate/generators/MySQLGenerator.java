package com.eroi.migrate.generators;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.eroi.migrate.misc.SchemaMigrationException;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;

/**
  * <p>Class MySQLGenerator provides methods for creating statements to 
  * create, alter, or drop tables.</p>
  */
public class MySQLGenerator extends AbstractGenerator {
    
	public String addIndex(Index index) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public String createTableStatement(Table table, String options) {
		// TODO Auto-generated method stub
		return null;
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

	StringBuffer retVal = new StringBuffer();

	Column[] columns = table.getColumns();

	if (columns == null || columns.length == 0) {
	    throw new SchemaMigrationException("Table must include at least one column");
	}

	int numberOfKeyColumns = GeneratorHelper.countPrimaryKeyColumns(columns);
	if (numberOfKeyColumns !=1) {
	    throw new SchemaMigrationException("Compound primary key support is not implemented yet.  Each table must have one and only one primary key.  You included " + numberOfKeyColumns);
	}

	int numberOfAutoIncrementColumns = GeneratorHelper.countAutoIncrementColumns(columns);
	if (numberOfAutoIncrementColumns > 1) {
	    throw new SchemaMigrationException("Each table can have at most one auto_increment key.  You included " + numberOfAutoIncrementColumns);
	}

	retVal.append("create table if not exists ")
	      .append(wrapName(table.getTableName()))
	      .append(" (");

	try {
	    for (int x = 0; x < columns.length; x++) {
		Column column = (Column)columns[x];

		if (x > 0) {
		    retVal.append(", ");
		}

		retVal.append(makeColumnString(column));
	    }
	} catch (ClassCastException e) {
	    throw new SchemaMigrationException("A table column couldn't be cast to a column: " + e.getMessage());
	}

	return retVal.toString().trim() + ");";
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
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
	    throw new SchemaMigrationException ("Must provide a table to add the column to");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column) + ";");

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
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
	    throw new SchemaMigrationException ("Must provide a table to add the column too");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column))
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
	    throw new SchemaMigrationException("Must include a non-null column");
	}

	if (table == null) {
	    throw new SchemaMigrationException ("Must provide a table to add the column too");
	}

	StringBuffer retVal = new StringBuffer();

	retVal.append("alter table ")
	      .append(wrapName(table.getTableName()))
	      .append(" add ")
	      .append(makeColumnString(column))
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
    protected String makeColumnString(Column column) {
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

	if (column.isPrimaryKey()) {
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
	Column[] columns = null;
	try {
	    Statement statement = connection.createStatement();
	    ResultSet rs = statement.executeQuery("SHOW CREATE TABLE `" + tableName + "`;");
	    int autoincrementColumn = 0;
	    int autoincrementCount = 0;
	    while (rs.next()) {
		String[] showCreateTableString = rs.getString(2).split("\n");
		for (int i = 0; i < showCreateTableString.length; i++) {
		    if (showCreateTableString[i].indexOf("auto_increment") > 0) {
			autoincrementColumn = i;
			autoincrementCount++;
		    }
		}
	    }
	    if (autoincrementCount > 1) {
		throw new SchemaMigrationException("Each table can have at most one auto_increment key.  You included " + autoincrementCount);
	    }
	    DatabaseMetaData dmd = connection.getMetaData();
	    rs= dmd.getPrimaryKeys(catalogName, null, tableName);
	    rs.last();
	    if (rs.getRow() != 1) {
		throw new SchemaMigrationException("Compound primary key support is not implemented yet.  Each table must have one and only one primary key.");
	    }
	    rs.beforeFirst();
	    String primaryKeyColumnName = null;
	    while (rs.next()) {
		primaryKeyColumnName = rs.getString(4);
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
	    while (rs.next()) {
		columnName = rs.getString(4);
		if (columnName.equalsIgnoreCase(primaryKeyColumnName)) {
		    primaryKeyColumn = true;
		}
		else {
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
	}
	return new Table(tableName, columns);
    }
}
