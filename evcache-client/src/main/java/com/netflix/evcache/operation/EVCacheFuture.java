package com.netflix.evcache.operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netflix.evcache.pool.ServerGroup;

public class EVCacheFuture implements Future<Boolean> {

    private final Future<Boolean> future;
    private final String app;
    private final ServerGroup serverGroup;
    private final String key;

    public EVCacheFuture(Future<Boolean> future, String key, String app, ServerGroup serverGroup) {
        this.future = future;
        this.app = app;
        this.serverGroup = serverGroup;
        this.key = key;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
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

    public String getServerGroupNamae() {
        return serverGroup.getName();
    }

    @Override
    public String toString() {
        return "EVCacheFuture [future=" + future + ", app=" + app + ", ServerGroup="
                + serverGroup + "]";
    }

}