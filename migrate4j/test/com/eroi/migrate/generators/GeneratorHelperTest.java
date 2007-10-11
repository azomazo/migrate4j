package com.eroi.migrate.generators;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.eroi.migrate.SchemaBuilder;
import com.eroi.migrate.SchemaElement;

import junit.framework.TestCase;

public class GeneratorHelperTest extends TestCase {

	public void testGetSqlName() {
		assertEquals("BIGINT", GeneratorHelper.getSqlName(Types.BIGINT));
		assertEquals("BOOL", GeneratorHelper.getSqlName(Types.BOOLEAN));
		assertEquals("CHAR", GeneratorHelper.getSqlName(Types.CHAR));
		assertEquals("DATE", GeneratorHelper.getSqlName(Types.DATE));
		assertEquals("DECIMAL", GeneratorHelper.getSqlName(Types.DECIMAL));
		assertEquals("DOUBLE", GeneratorHelper.getSqlName(Types.DOUBLE));
		assertEquals("FLOAT", GeneratorHelper.getSqlName(Types.FLOAT));
		assertEquals("INT", GeneratorHelper.getSqlName(Types.INTEGER));
		assertEquals("NUMERIC", GeneratorHelper.getSqlName(Types.NUMERIC));
		assertEquals("SMALLINT", GeneratorHelper.getSqlName(Types.SMALLINT));
		assertEquals("TIME", GeneratorHelper.getSqlName(Types.TIME));
		assertEquals("TIMESTAMP", GeneratorHelper.getSqlName(Types.TIMESTAMP));
		assertEquals("TINYINT", GeneratorHelper.getSqlName(Types.TINYINT));
		assertEquals("VARCHAR", GeneratorHelper.getSqlName(Types.VARCHAR));
		
		assertNull(GeneratorHelper.getSqlName(Integer.MAX_VALUE));
	}
	
	public void testNeedsLength() {
		assertTrue(GeneratorHelper.needsLength(Types.CHAR));
		assertTrue(GeneratorHelper.needsLength(Types.VARCHAR));
		
		assertFalse(GeneratorHelper.needsLength(Types.DATE));
		assertFalse(GeneratorHelper.needsLength(Types.TIMESTAMP));
	}
	
	public void testNeedsQuotes() {
		assertTrue(GeneratorHelper.needsQuotes(Types.CHAR));
		assertTrue(GeneratorHelper.needsQuotes(Types.VARCHAR));
		
		assertFalse(GeneratorHelper.needsQuotes(Types.DATE));
		assertFalse(GeneratorHelper.needsQuotes(Types.TIMESTAMP));
	}
	
	public void testCountPrimaryKeyColumns() {
		List columns = new ArrayList();
		
		columns.add(SchemaBuilder.createPrimaryKeyColumn("primary1", Types.INTEGER, true));
		columns.add(SchemaBuilder.createColumn("column1", Types.INTEGER));
		columns.add(SchemaBuilder.createColumn("column2", Types.INTEGER));
		
		SchemaElement.Column[] columnArray = 
			(SchemaElement.Column[])columns.toArray(new SchemaElement.Column[columns.size()]);
		
		assertEquals(1, GeneratorHelper.countPrimaryKeyColumns(columnArray));
		
		columns.add(SchemaBuilder.createPrimaryKeyColumn("primary2", Types.INTEGER, false));
		
		columnArray = 
			(SchemaElement.Column[])columns.toArray(new SchemaElement.Column[columns.size()]);
		
		assertEquals(2, GeneratorHelper.countPrimaryKeyColumns(columnArray));
	}
}
