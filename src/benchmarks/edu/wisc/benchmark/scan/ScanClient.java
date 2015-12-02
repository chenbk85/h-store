package edu.wisc.benchmark.scan;

import java.io.IOException;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
 
public class ScanClient extends BenchmarkComponent {
	
	private long a_id = 0;
 
    public static void main(String args[]) {
        BenchmarkComponent.main(ScanClient.class, args, false);
    }
 
    public ScanClient(String[] args) {
        super(args);
        /*for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR */
    }
 
   /* @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            Random rand = new Random();
            while (true) {
                // Select a random transaction to execute and generate its input parameters
                // The procedure index (procIdx) needs to the same as the array of procedure
                // names returned by getTransactionDisplayNames()
                int procIdx = rand.nextInt(SeqProjectBuilder.PROCEDURES.length);
                String procName = SeqProjectBuilder.PROCEDURES[procIdx].getSimpleName();
                long a_id = rand.nextInt(1000000); // TODO remove this hard-coded number
                Object procParams[] = new Object[] { a_id } ; // TODO
 
                // Create a new Callback handle that will be executed when the transaction completes
                Callback callback = new Callback(procIdx);
 
                // Invoke the stored procedure through the client handle. This is non-blocking
                client.callProcedure(callback, procName, procParams);
 
                // Check whether all the nodes are backed-up and this client should block
                // before sending new requests. 
                client.backpressureBarrier();
            } // WHILE
        } catch (NoConnectionsException e) {
            // Client has no clean mechanism for terminating with the DB.
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // At shutdown an IOException is thrown for every connection to
            // the DB that is lost Ignore the exception here in order to not
            // get spammed, but will miss lost connections at runtime
        }
    }*/
    
    
    @Override
    protected boolean runOnce() throws IOException {
        // pick random transaction to call
    	Client client = this.getClientHandle();
        Random rand = new Random();
        
        int procIdx = 0;
        String procName = ScanProjectBuilder.PROCEDURES[procIdx].getSimpleName();
        a_id = (a_id++) % 1000000L; // TODO remove this hard-coded number
        Object procParams[] = new Object[] { a_id } ; // TODO
        
        Callback callback = new Callback(procIdx);
        boolean val = this.getClientHandle().callProcedure(callback, procName, procParams);
        
        return val;
    }
 
    @SuppressWarnings("unused")
    @Deprecated
    @Override
    public void runLoop() {
    	Client client = this.getClientHandle();
        try {
            while (true) {
                this.runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
 
    private class Callback implements ProcedureCallback {
        private final int idx;
 
        public Callback(int idx) {
            this.idx = idx;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
        }
    } // END CLASS
 
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[ScanProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = ScanProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
