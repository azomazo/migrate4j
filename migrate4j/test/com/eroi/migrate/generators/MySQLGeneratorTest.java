/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.eroi.migrate.generators;

import java.sql.Types;

import junit.framework.TestCase;

import com.eroi.migrate.schema.Column;

public class MySQLGeneratorTest extends TestCase {

    private MySQLGenerator generator;

    public MySQLGeneratorTest(String testName) {
        super(testName);
    }
    
    protected void setUp() throws Exception {
        super.setUp();
        generator = new MySQLGenerator(null);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testMakeColumnString_SimpleColumn() {
        Column column = new Column("basic", Types.INTEGER);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT", columnString);
    }

    public void testMakeColumnString_PrimaryKeyNonIncrementing() {
        Column column = new Column("basic", Types.INTEGER, -1, true, false, null, false);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT NOT NULL PRIMARY KEY", columnString);
    }

    public void testMakeColumnString_PrimaryKeyIncrementing() {
        Column column = new Column("basic", Types.INTEGER, -1, true, false, null, true);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT NOT NULL AUTO_INCREMENT PRIMARY KEY", columnString);
    }

    public void testMakeColumnString_VarcharWithDefault() {
        Column column = new Column("basic", Types.VARCHAR, 50, false, false, "NA", false);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` VARCHAR(50) NOT NULL DEFAULT 'NA'", columnString);
    }

    
    /*public void testAddIndex() {
        String expected = "alter table `Person` add index `idx_Person_id_name` (`id`, `name`);";
        Index index = new Index("Person", new String[]{"id", "name"});
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    public void testAddIndex_Unique() {
        String expected = "alter table `Person` add unique index `index1` (`id`, `name`);";
        Index index = new Index("index1", "Person", new String[]{"id", "name"}, true, false);
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    public void testAddPrimaryKey() {
        Index index = new Index("primaryIndex", "Person", new String[]{"id", "name"}, false, true);
        String expected = "alter table `Person` add constraint `primaryIndex` PRIMARY KEY(`id`, `name`);";
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    public void testDropIndex() {
        Index index = new Index("Person", new String[]{"id", "name"});
        String expected = "alter table `Person` drop INDEX `idx_Person_id_name`;";
        String result = generator.dropIndex(index);
        assertEquals(expected, result);
    }

    public void testDropPrimaryKey() {
        Index index = new Index("primaryIndex", "Person", new String[]{"id", "name"}, false, true);
        String expected = "alter table `Person` drop PRIMARY KEY;";
        String result = generator.dropIndex(index);
        assertEquals(expected, result);
    }

    public void testCreateTableStatement() {
        String expected = "create table if not exists `sample` (`id` INT NOT NULL PRIMARY KEY, `desc` VARCHAR(50) NOT NULL);";

        Column[] columns = new Column[2];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, false);
        columns[1] = new Column("desc", Types.VARCHAR, 50, false, false, null, false);
        Table table = Define.table("sample", columns);
        String tableString = generator.createTableStatement(table);
        assertEquals(expected, tableString);
    }

    public void testCreateTableStatement_Options() {
        String expected = "create table if not exists `sample` " +
                "(`id` INT NOT NULL PRIMARY KEY, `desc` VARCHAR(50) NOT NULL) " +
                "Engine = InnoDB;";
        Column[] columns = new Column[2];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, false);
        columns[1] = new Column("desc", Types.VARCHAR, 50, false, false, null, false);
        Table table = Define.table("sample", columns);
        String tableString = generator.createTableStatement(table, "Engine = InnoDB");
        assertEquals(expected, tableString);
    }

    public void testDropTableStatement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Table table = Define.table("sample", columns);
        String expected = "drop table if exists `sample`;";
        String result = generator.dropTableStatement(table.getTableName());
        assertEquals(expected, result);
    }

    public void testAddForeignKey() {
        ForeignKey foreignKey = new ForeignKey("name", "product",
                new String[]{"category", "id"}, "product_order",
                new String[]{"product_category", "product_id"});
        ;
        String expected = "alter table `product_order` add constraint " +
                "`name` foreign key (`product_category`, " +
                "`product_id`) references `product` (`category`, `id`);";
        String result = generator.addForeignKey(foreignKey);
        assertEquals(expected, result);
    }

    public void testDropForeignKey() {
        ForeignKey foreignKey = new ForeignKey("name", "product",
                new String[]{"category", "id"}, "product_order",
                new String[]{"product_category", "product_id"});
        ;
        String expected = "alter table `product_order` drop foreign key " +
                "`name`;";
        String result = generator.dropForeignKey(foreignKey);
        assertEquals(expected, result);
    }*/
}
