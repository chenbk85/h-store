package edu.brown.workload;

import java.io.File;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestWorkloadAnalyzer extends BaseTestCase {
	protected static final int WORKLOAD_XACT_LIMIT = 1000;

	public void testCountOfReferences() throws Exception {
		super.setUp(ProjectType.TM1);

		Workload workload;
		File workload_file = this.getWorkloadFile(ProjectType.TM1);
		workload = new Workload(catalog);
		((Workload) workload).load(workload_file, catalog_db, null);
		assert (workload.getTransactionCount() > 0) : "No transaction loaded from workload";

		WorkloadAnalyzer analyzer = new WorkloadAnalyzer(this.getDatabase(),
				workload);
		int timeInterval = 20000;
		int result = analyzer.getCountOfReferencesInInterval(timeInterval);

		assertNotNull(result);
		assertEquals(2210, result);
	}

}
