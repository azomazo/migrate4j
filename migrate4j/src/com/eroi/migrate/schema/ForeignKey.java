package com.eroi.migrate.schema;

public class ForeignKey {
    
    private Table parentTable;
    private Column[] parentColumns;
    private Table childTable;
    private Column[] childColumns;
    
    public ForeignKey(Table parentTable, Column parentColumn, Table childTable, Column childColumn) {
        this.parentTable = parentTable;
        this.childTable = childTable;
        this.parentColumns = new Column[] { parentColumn };
        this.childColumns = new Column[] { childColumn };
    }
    
    public ForeignKey(Table parentTable, Column[] parentColumns, Table childTable, Column[] childColumns) {
        this.parentTable = parentTable;
        this.childTable = childTable;
        this.parentColumns = parentColumns;
        this.childColumns = childColumns;
    }

    public Table getParentTable() {
        return parentTable;
    }

    public Column[] getParentColumns() {
        return parentColumns;
    }

    public Table getChildTable() {
        return childTable;
    }

    public Column[] getChildColumns() {
        return childColumns;
    }
    
    
}
