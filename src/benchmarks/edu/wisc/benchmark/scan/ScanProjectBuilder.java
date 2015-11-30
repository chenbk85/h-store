package edu.wisc.benchmark.scan;

import org.voltdb.VoltProcedure;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.example.procedures.GetData;

public class ScanProjectBuilder extends AbstractProjectBuilder {
	
	public static final Class<? extends BenchmarkComponent> m_clientClass = ScanClient.class;
	
	public static final Class<? extends BenchmarkComponent> m_loaderClass = ScanLoader.class;
	
	public static final Class<?> PROCEDURES[] = new Class<?>[] {
		GetData.class
	};
	
	public static final String PARTITIONING[][] = new String[][] {
		{ "TABLEA", "A_ID" }
	};
	
	@SuppressWarnings("unchecked")
	public ScanProjectBuilder() {
		super("scan", ScanProjectBuilder.class, (Class<? extends VoltProcedure>[]) PROCEDURES, PARTITIONING);
		
		addStmtProcedure("DeleteData", "DELETE FROM TABLEA WHERE A_ID < ?");
	}
	
}
