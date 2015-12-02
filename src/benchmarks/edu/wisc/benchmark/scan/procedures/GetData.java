package edu.wisc.benchmark.scan.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class GetData extends VoltProcedure {
	
	public final SQLStmt GetA = new SQLStmt("SELECT * FROM TABLEA WHERE A_VALUE_SEQ = ?");
	
	public VoltTable[] run(long a_id) {
		voltQueueSQL(GetA, a_id);
		return (voltExecuteSQL());
	}

}
