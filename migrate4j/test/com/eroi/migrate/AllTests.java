package com.eroi.migrate;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.eroi.migrate.generators.GeneratorHelperTest;
import com.eroi.migrate.generators.GenericGeneratorTest;
import com.eroi.migrate.generators.MySQLGeneratorTest;
import com.eroi.migrate.schema.ForeignKeyTest;
import com.eroi.migrate.schema.IndexTest;
import com.eroi.migrate.version.TestVersionMigration;
import com.eroi.validation.GeneratorValidationTest;

public class AllTests {

	public static Test suite() {
		TestSuite suite = new TestSuite("Test for com.eroi.migrate");
		//$JUnit-BEGIN$
		suite.addTestSuite(ConfigureTest.class);
		suite.addTestSuite(DefineTest.class);
		suite.addTestSuite(EndToEndTest.class);
		suite.addTestSuite(EngineTest.class);
		suite.addTestSuite(SampleMigrationTest.class);
		
		suite.addTestSuite(ForeignKeyTest.class);
		suite.addTestSuite(IndexTest.class);

		suite.addTestSuite(GeneratorHelperTest.class);
		suite.addTestSuite(GenericGeneratorTest.class);
		suite.addTestSuite(MySQLGeneratorTest.class);

		suite.addTestSuite(GeneratorValidationTest.class);
		
		suite.addTestSuite(TestVersionMigration.class);
		//$JUnit-END$
		return suite;
	}

}
