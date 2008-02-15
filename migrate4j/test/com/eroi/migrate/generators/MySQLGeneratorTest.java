/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.eroi.migrate.generators;

import com.eroi.migrate.Define;
import com.eroi.migrate.schema.Column;
import com.eroi.migrate.schema.ForeignKey;
import com.eroi.migrate.schema.Index;
import com.eroi.migrate.schema.Table;
import junit.framework.TestCase;
import java.sql.Types;

public class MySQLGeneratorTest extends TestCase {

    private MySQLGenerator generator;

    public MySQLGeneratorTest(String testName) {
        super(testName);
    }
    
    protected void setUp() throws Exception {
        super.setUp();
        generator = new MySQLGenerator();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testMakeColumnString_SimpleColumn() {
        Column column = Define.column("basic", Types.INTEGER);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT NULL", columnString);
    }

    public void testMakeColumnString_PrimaryKeyNonIncrementing() {
        Column column = Define.column("basic", Types.INTEGER, -1, true, false, null, false);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT NOT NULL PRIMARY KEY", columnString);
    }

    public void testMakeColumnString_PrimaryKeyIncrementing() {
        Column column = Define.column("basic", Types.INTEGER, -1, true, false, null, true);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` INT NOT NULL AUTO_INCREMENT PRIMARY KEY", columnString);
    }

    public void testMakeColumnString_VarcharWithDefault() {
        Column column = Define.column("basic", Types.VARCHAR, 50, false, false, "NA", false);
        String columnString = generator.makeColumnString(column, false);
        assertEquals("`basic` VARCHAR(50) NOT NULL DEFAULT 'NA'", columnString);
    }

    /**
     * Test of addIndex method, of class MySQLGenerator.
     */
    public void testAddIndex() {
        String expected = "alter table `Person` add index `idx_Person_id_name` (`id`, `name`);";
        Index index = new Index("Person", new String[]{"id", "name"});
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    /**
     * Test of addIndex method, of class MySQLGenerator.
     */
    public void testAddIndex_Unique() {
        String expected = "alter table `Person` add unique index `index1` (`id`, `name`);";
        Index index = new Index("index1", "Person", new String[]{"id", "name"}, true, false);
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    /**
     * Test of addPrimaryKey method, of class MySQLGenerator.
     */
    public void testAddPrimaryKey() {
        Index index = new Index("primaryIndex", "Person", new String[]{"id", "name"}, false, true);
        String expected = "alter table `Person` add constraint `primaryIndex` PRIMARY KEY(`id`, `name`);";
        String result = generator.addIndex(index);
        assertEquals(expected, result);
    }

    /**
     * Test of dropIndex method, of class MySQLGenerator.
     */
    public void testDropIndex() {
        Index index = new Index("Person", new String[]{"id", "name"});
        String expected = "alter table `Person` drop INDEX `idx_Person_id_name`;";
        String result = generator.dropIndex(index);
        assertEquals(expected, result);
    }

    /**
     * Test of dropPrimaryKey method, of class MySQLGenerator.
     */
    public void testDropPrimaryKey() {
        Index index = new Index("primaryIndex", "Person", new String[]{"id", "name"}, false, true);
        String expected = "alter table `Person` drop PRIMARY KEY;";
        String result = generator.dropIndex(index);
        assertEquals(expected, result);
    }

    /**
     * Test of createTableStatement method, of class MySQLGenerator.
     */
    public void testCreateTableStatement() {
        String expected = "create table if not exists `sample` (`id` INT NOT NULL PRIMARY KEY, `desc` VARCHAR(50) NOT NULL);";

        Column[] columns = new Column[2];
        columns[0] = Define.column("id", Types.INTEGER, -1, true, false, null, false);
        columns[1] = Define.column("desc", Types.VARCHAR, 50, false, false, null, false);
        Table table = Define.table("sample", columns);
        String tableString = generator.createTableStatement(table);
        assertEquals(expected, tableString);
    }

    /**
     * Test of createTableStatement method, of class MySQLGenerator.
     */
    public void testCreateTableStatement_Options() {
        String expected = "create table if not exists `sample` " +
                "(`id` INT NOT NULL PRIMARY KEY, `desc` VARCHAR(50) NOT NULL) " +
                "Engine = InnoDB;";
        Column[] columns = new Column[2];
        columns[0] = Define.column("id", Types.INTEGER, -1, true, false, null, false);
        columns[1] = Define.column("desc", Types.VARCHAR, 50, false, false, null, false);
        Table table = Define.table("sample", columns);
        String tableString = generator.createTableStatement(table, "Engine = InnoDB");
        assertEquals(expected, tableString);
    }

    /**
     * Test of addColumnStatement method, of class MySQLGenerator.
     */
    public void testAddColumnStatement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Column column = new Column("desc", Types.VARCHAR, 100, false, true, null, false);
        Table table = Define.table("sample", columns);
        String expected = "alter table `sample` add `desc` VARCHAR(100) NULL;";
        String result = generator.addColumnStatement(column, table);
        assertEquals(expected, result);
    }

    /**
     * Test of addColumnFirstStatement method, of class MySQLGenerator.
     */
    public void testAddColumnFirstStatement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Column column = new Column("desc", Types.VARCHAR, 100, false, true, null, false);
        Table table = Define.table("sample", columns);
        String expected = "alter table `sample` add `desc` VARCHAR(100) NULL FIRST;";
        String result = generator.addColumnFirstStatement(column, table);
        assertEquals(expected, result);
    }

    /**
     * Test of addColumnAfterStatement method, of class MySQLGenerator.
     */
    public void testAddColumnAfterStatement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Column column = new Column("desc", Types.VARCHAR, 100, false, true, null, false);
        Table table = Define.table("sample", columns);
        String expected = "alter table `sample` add `desc` VARCHAR(100) NULL AFTER `id`;";
        String result = generator.addColumnAfterStatement(column, table, "id");
        assertEquals(expected, result);
    }

    /**
     * Test of alterEngine method, of class MySQLGenerator.
     */
    public void testAlterEngine() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Table table = Define.table("sample", columns);
        String expected = "alter table `sample` engine = `InnoDB`;";
        String result = generator.alterEngine(table, "InnoDB");
        assertEquals(expected, result);
    }

    /**
     * Test of alterAutoincrement method, of class MySQLGenerator.
     */
    public void testAlterAutoincrement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Table table = Define.table("sample", columns);
        String expected = "alter table `sample` auto_increment = 101;";
        String result = generator.alterAutoincrement(table, 101);
        assertEquals(expected, result);
    }

    /**
     * Test of dropTableStatement method, of class MySQLGenerator.
     */
    public void testDropTableStatement() {
        Column[] columns = new Column[1];
        columns[0] = new Column("id", Types.INTEGER, -1, true, false, null, true);
        Table table = Define.table("sample", columns);
        String expected = "drop table if exists `sample`;";
        String result = generator.dropTableStatement(table);
        assertEquals(expected, result);
    }

    /**
     * Test of addForeignKey method, of class MySQLGenerator.
     */
    public void testAddForeignKey() {
        ForeignKey foreignKey = new ForeignKey("product",
                new String[]{"category", "id"}, "product_order",
                new String[]{"product_category", "product_id"});
        ;
        String expected = "alter table `product_order` add constraint " +
                "`fky_produ_cate_id_produ` foreign key (`product_category`, " +
                "`product_id`) references `product` (`category`, `id`);";
        String result = generator.addForeignKey(foreignKey);
        assertEquals(expected, result);
    }

    /**
     * Test of dropForeignKey method, of class MySQLGenerator.
     */
    public void testDropForeignKey() {
        ForeignKey foreignKey = new ForeignKey("product",
                new String[]{"category", "id"}, "product_order",
                new String[]{"product_category", "product_id"});
        ;
        String expected = "alter table `product_order` drop foreign key " +
                "`fky_produ_cate_id_produ`;";
        String result = generator.dropForeignKey(foreignKey);
        assertEquals(expected, result);
    }
}
