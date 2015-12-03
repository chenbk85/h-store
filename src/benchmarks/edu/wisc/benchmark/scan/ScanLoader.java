package edu.wisc.benchmark.scan;


import java.io.IOException;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;

public class ScanLoader extends Loader {
	
	public static void main(String args[]) throws Exception {
		BenchmarkComponent.main(ScanLoader.class, args, true);
    }

	public ScanLoader(String[] args) {
		super(args);
		/*for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR*/
	}

	@Override
	public void load() throws IOException {
		// The catalog contains all the information about the database (e.g., tables, columns, indexes)
        // It is loaded from the benchmark's project JAR file
		final Database catalog_db = this.getCatalogContext().database;
 
        // Iterate over all of the Table handles in the catalog and generate
        // tuples to upload into the database
        for (Table catalog_tbl : catalog_db.getTables()) {
            // TODO: Create an empty VoltTable handle and then populate it in batches to 
            //       be sent to the DBMS
            VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
            
            long totalSize = 40000000L;
            long batchSize = 1000;
            long randomSeed = 0xFFFF;
            
            Random randomGenerator = new Random();
            randomGenerator.setSeed(randomSeed);
            
            for (long s_id = 0; s_id < totalSize; s_id++) {
                    Object row[] = new Object[table.getColumnCount()];
                    row[0] = s_id; // indexed 
                    row[1] = s_id; // non-indexed
                    row[2] = randomGenerator.nextInt();
                    table.addRow(row);
                  if (table.getRowCount() >= batchSize) {
                	// Invoke the BenchmarkComponent's data loading method
                    // This will upload the contents of the VoltTable into the DBMS cluster
                    loadVoltTable(catalog_tbl.getName(), table);
                    table.clearRowData();
                }
            } // WHILE
            if (table.getRowCount() > 0) {
            	// Invoke the BenchmarkComponent's data loading method
                // This will upload the contents of the VoltTable into the DBMS cluster
                loadVoltTable(catalog_tbl.getName(), table);
                table.clearRowData();
            }            
        } // FOR
	}

}
