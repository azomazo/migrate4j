package com.eroi.migrate;

public interface DDLGenerator {

	public String getCreateTableStatement(SchemaElement.Table table);

	public String getDropStatement(SchemaElement element);
}
