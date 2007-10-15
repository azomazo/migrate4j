package com.eroi.migrate;


/**
 * Represents changes to a database schema.
 * Migrations are applied in the order of their
 * name, which must follow a "Migration_X" pattern
 * where X indicates the order.
 * 
 */
public interface Migration {

	/**
	 * Indicates what this migration does.  Since
	 * migrate4j requires that all Migration
	 * implementors use the "Migration_X" 
	 * naming pattern, getDescription replaces
	 * the ability of providing a descriptive
	 * name.  It's also a good idea to add
	 * descriptive javadoc class comments
	 * on your Migration implements.
	 * 
	 * @return String 
	 */
	public String getDescription();
	
	/**
	 * Work to perform when upgrading the database
	 * schema
	 */
	public void up();
	
	
	/**
	 * Work to perform when reverting the database
	 * schema to a previous version
	 */
	public void down();
	
}
