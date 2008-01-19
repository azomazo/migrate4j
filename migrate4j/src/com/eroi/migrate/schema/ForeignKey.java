package com.eroi.migrate.schema;

import com.eroi.migrate.misc.SchemaMigrationException;

public class ForeignKey {
    
	private String name;
    private String parentTable;
    private String[] parentColumns;
    private String childTable;
    private String[] childColumns;
    
    public ForeignKey(String name, String parentTable, String parentColumn, String childTable, String childColumn) {
    	this(name, parentTable, new String[] { parentColumn }, childTable, new String[] { childColumn });
    }
    
    public ForeignKey(String name, String parentTable, String[] parentColumns, String childTable, String[] childColumns) {
        
    	this.name = name;
    	this.parentTable = parentTable;
        this.childTable = childTable;
        this.parentColumns = parentColumns;
        this.childColumns = childColumns;
        
        init();
    }
    
    private void init() {
    	
    	if (parentTable == null || 
    			parentColumns == null || 
    			parentColumns.length == 0 || 
    			!ConstraintHelper.hasValidValue(parentColumns) ||
    			childTable == null || 
    			childColumns == null || 
    			childColumns.length == 0 || 
    			!ConstraintHelper.hasValidValue(childColumns)) {
			throw new SchemaMigrationException("Must provide a table and columns to use for index");
		}
    	
    	if (name == null || name.trim().length() == 0) {
        	name = createName();
        }
    }
    
    public ForeignKey(String parentTable, String parentColumn, String childTable, String childColumn) {
        this(null, parentTable, parentColumn, childTable, childColumn);
    }
    
    public ForeignKey(String parentTable, String[] parentColumns, String childTable, String[] childColumns) {
    	this(null, parentTable, parentColumns, childTable, childColumns);
    }

    public String getName() {
		return name;
	}
    
    public String getParentTable() {
        return parentTable;
    }

    public String[] getParentColumns() {
        return parentColumns;
    }

    public String getChildTable() {
        return childTable;
    }

    public String[] getChildColumns() {
        return childColumns;
    }
    
    private String createName() {
    	StringBuffer name = new StringBuffer();
    	
    	name.append("fky_")
    		.append(ConstraintHelper.nameFromTable(parentTable, 5))
    		.append("_")
    		.append(ConstraintHelper.nameFromColumns(parentColumns))
    		.append("_")
    		.append(ConstraintHelper.nameFromTable(childTable, 5));
    	
    	return name.toString();
    }
}
