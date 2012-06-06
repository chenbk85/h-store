package edu.brown.hstore.util;

import java.util.concurrent.LinkedBlockingQueue;

import org.voltdb.utils.Pair;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Shutdownable;

public class TransactionDispatcher implements Runnable, Shutdownable {

    private final HStoreSite hstore_site;
    private HStoreConf hstore_conf;
    private final LinkedBlockingQueue<Pair<byte[], RpcCallback<byte[]>>> queue = new LinkedBlockingQueue<Pair<byte[],RpcCallback<byte[]>>>();
    private Shutdownable.ShutdownState state = null;
    
    public TransactionDispatcher(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
    }
    
    public void queue(byte[] serializedRequest, RpcCallback<byte[]> done) {
        this.queue.offer(Pair.of(serializedRequest, done));
    }
    
    @Override
    public void run() {
        Thread self = Thread.currentThread();
        self.setName(HStoreThreadManager.getThreadName(hstore_site, "dispatch"));
        if (hstore_conf.site.cpu_affinity) {
            hstore_site.getThreadManager().registerProcessingThread();
        }
        
        Pair<byte[], RpcCallback<byte[]>> pair = null;
        while (this.state != ShutdownState.SHUTDOWN) {
            try {
                pair = this.queue.take();
            } catch (InterruptedException ex) {
                break;
            }
            if (this.state == ShutdownState.PREPARE_SHUTDOWN) {
                // TODO: Send back rejection
            } else {
                this.hstore_site.procedureInvocation(pair.getFirst(), pair.getSecond());
            }
        } // WHILE

    }
    
    @Override
    public void prepareShutdown(boolean error) {
        this.state = ShutdownState.PREPARE_SHUTDOWN;
    }

    @Override
    public void shutdown() {
        this.state = ShutdownState.SHUTDOWN;
    }

    @Override
    public boolean isShuttingDown() {
        return (this.state == ShutdownState.PREPARE_SHUTDOWN);
    }



}
