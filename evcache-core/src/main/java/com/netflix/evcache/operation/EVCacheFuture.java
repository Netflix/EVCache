package com.netflix.evcache.operation;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;

public class EVCacheFuture implements Future<Boolean> {

    private Logger log = LoggerFactory.getLogger(EVCacheFuture.class);
    private final Future<Boolean> future;
    private final String app;
    private final ServerGroup serverGroup;
    private final String key;
    private final EVCacheClient client;

    public EVCacheFuture(Future<Boolean> future, String key, String app, ServerGroup serverGroup) {
    	this(future, key, app, serverGroup, null);
    }

    public EVCacheFuture(Future<Boolean> future, String key, String app, ServerGroup serverGroup, EVCacheClient client) {
        this.future = future;
        this.app = app;
        this.serverGroup = serverGroup;
        this.key = key;
        this.client = client;
    }

    public Future<Boolean> getFuture() {
        return future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
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

    public String getServerGroupName() {
        return serverGroup.getName();
    }

    public EVCacheClient getEVCacheClient() {
        return client;
    }

    @Override
    public String toString() {
        return "EVCacheFuture [future=" + future + ", app=" + app + ", ServerGroup="
                + serverGroup + ", EVCacheClient=" + client + "]";
    }

}