package com.netflix.evcache.operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.pool.ServerGroup;

import net.spy.memcached.internal.ListenableFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "DE_MIGHT_IGNORE", "EI_EXPOSE_REP2" })
public class EVCacheFutures implements ListenableFuture<Boolean, OperationCompletionListener>,
        OperationCompletionListener {

    private final OperationFuture<Boolean>[] futures;
    private final String app;
    private final ServerGroup serverGroup;
    private final String key;
    private final AtomicInteger completionCounter;
    private final EVCacheLatch latch;

    public EVCacheFutures(OperationFuture<Boolean>[] futures, String key, String app, ServerGroup serverGroup,
            EVCacheLatch latch) {
        this.futures = futures;
        this.app = app;
        this.serverGroup = serverGroup;
        this.key = key;
        this.latch = latch;
        this.completionCounter = new AtomicInteger(futures.length);
        if (latch != null && latch instanceof EVCacheLatchImpl) ((EVCacheLatchImpl) latch).addFuture(this);
        for (int i = 0; i < futures.length; i++) {
            final OperationFuture<Boolean> of = futures[i];
            if (of.isDone()) {
                try {
                    onComplete(of);
                } catch (Exception e) {
                }
            } else {
                of.addListener(this);
            }
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        for (OperationFuture<Boolean> future : futures) {
            future.cancel();
        }
        return true;
    }

    @Override
    public boolean isCancelled() {

        for (OperationFuture<Boolean> future : futures) {
            if (future.isCancelled() == false) return false;
        }
        return true;
    }

    @Override
    public boolean isDone() {
        for (OperationFuture<Boolean> future : futures) {
            if (future.isDone() == false) return false;
        }
        return true;
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        for (OperationFuture<Boolean> future : futures) {
            if (future.get() == false) return false;
        }
        return true;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        for (OperationFuture<Boolean> future : futures) {
            if (future.get(timeout, unit) == false) return false;
        }
        return true;

    }

    public String getKey() {
        return key;
    }

    public String getApp() {
        return app;
    }

    public String getZone() {
        return serverGroup.getZone();
    }

    public String getReplicaSetNamae() {
        return serverGroup.getName();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("EVCacheFutures [futures=[");
        for (OperationFuture<Boolean> future : futures)
            sb.append(future);
        sb.append("], app=").append(app).append(", ReplicaSet=").append(serverGroup.toString()).append("]");
        return sb.toString();
    }

    @Override
    public void onComplete(OperationFuture<?> future) throws Exception {
        int val = completionCounter.decrementAndGet();
        if (val == 0) {
            if (latch != null) latch.onComplete(future);// Pass the last future to get completed
        }
    }

    @Override
    public Future<Boolean> addListener(OperationCompletionListener listener) {
        return this;
    }

    @Override
    public Future<Boolean> removeListener(OperationCompletionListener listener) {
        return this;
    }

}