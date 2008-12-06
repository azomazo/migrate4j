package com.eroi.migrate.schema;

import com.eroi.migrate.misc.Validator;

public class ForeignKey {
    
	private String name;
    private String parentTable;
    private String[] parentColumns;
    private String childTable;
    private String[] childColumns;
    private CascadeRule cascadeDeleteRule;
    private CascadeRule cascadeUpdateRule;
    
    public ForeignKey(String name, String parentTable, String parentColumn, String childTable, String childColumn) {
    	this(name, parentTable, parentColumn, childTable, childColumn, CascadeRule.none,  CascadeRule.none);
    }
    
    public ForeignKey(String name, String parentTable, String parentColumn, String childTable, String childColumn, CascadeRule deleteRule, CascadeRule updateRule) {
    	this(name, parentTable, new String[] { parentColumn }, childTable, new String[] { childColumn }, deleteRule, updateRule);
    }

    public ForeignKey(String name, String parentTable, String[] parentColumns, String childTable, String[] childColumns) {
    	this(name, parentTable, parentColumns, childTable, childColumns, CascadeRule.none, CascadeRule.none);
    }

    public ForeignKey(String name, String parentTable, String[] parentColumns, String childTable, String[] childColumns, CascadeRule cascadeDeleteRule, CascadeRule cascadeUpdateRule) {
        
    	this.name = name;
    	this.parentTable = parentTable;
        this.childTable = childTable;
        this.parentColumns = parentColumns;
        this.childColumns = childColumns;
        this.cascadeDeleteRule = cascadeDeleteRule;
        this.cascadeUpdateRule = cascadeUpdateRule;
        
        init();
    }
    
    private void init() {
    	
    	Validator.notNull(name, "Name can not be null");
    	Validator.isTrue(name.trim().length() > 0, "Name can not be empty");
    	Validator.notNull(parentTable, "Parent table can not be null");
    	Validator.notNull(parentColumns, "Parent Columns can not be null");
    	Validator.isTrue(parentColumns.length > 0, "Must include at least one parent column");
    	Validator.isTrue(ConstraintHelper.hasValidValue(parentColumns), "Parent columns must include at least one valid column name");
    	Validator.notNull(childTable, "Child table can not be null");
    	Validator.notNull(childColumns, "Child columns can not be null");
    	Validator.isTrue(childColumns.length > 0, "Must include at least one child column");
    	Validator.isTrue(ConstraintHelper.hasValidValue(childColumns), "Child columns must include at least one valid column name");
    	
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
    
    public CascadeRule getCascadeDeleteRule() {
    	return cascadeDeleteRule;
    }

    public CascadeRule getCascadeUpdateRule() {
    	return cascadeUpdateRule;
    }
    
    public static String createName(String parentTableName, String childTableName, String[] parentColumnNames) {
    	StringBuffer name = new StringBuffer();
    	
    	name.append("fky_")
    		.append(ConstraintHelper.nameFromTable(parentTableName, 5))
    		.append("_")
    		.append(ConstraintHelper.nameFromColumns(parentColumnNames))
    		.append("_")
    		.append(ConstraintHelper.nameFromTable(childTableName, 5));
    	
    	return name.toString();
    }
}
