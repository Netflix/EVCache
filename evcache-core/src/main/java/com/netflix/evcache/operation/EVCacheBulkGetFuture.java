package com.netflix.evcache.operation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import com.sun.management.GcInfo;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import rx.Scheduler;
import rx.Single;

/**
 * Future for handling results from bulk gets.
 *
 * Not intended for general use.
 *
 * types of objects returned from the GETBULK
 */
@SuppressWarnings("restriction")
public class EVCacheBulkGetFuture<T> extends BulkGetFuture<T> {

    private Logger log = LoggerFactory.getLogger(EVCacheBulkGetFuture.class);
    private final Map<String, Future<T>> rvMap;
    private final Collection<Operation> ops;
    private final CountDownLatch latch;
    private final long start;
    private final EVCacheClient client;

    public EVCacheBulkGetFuture(Map<String, Future<T>> m, Collection<Operation> getOps, CountDownLatch l, ExecutorService service, EVCacheClient client) {
        super(m, getOps, l, service);
        rvMap = m;
        ops = getOps;
        latch = l;
        this.start = System.currentTimeMillis();
        this.client = client;
    }

    public Map<String, T> getSome(long to, TimeUnit unit, boolean throwException, boolean hasZF)
            throws InterruptedException, ExecutionException {
        boolean status = latch.await(to, unit);
        if(log.isDebugEnabled()) log.debug("Took " + (System.currentTimeMillis() - start)+ " to fetch " + rvMap.size() + " keys from " + client);
        long gcDuration = -1;
        List<Tag> tagList = null;
        Collection<Operation> timedoutOps = null;
        String statusString = EVCacheMetricsFactory.SUCCESS;

        try {
            if (!status) {
                boolean gcPause = false;
                tagList = new ArrayList<Tag>(6);
                tagList.addAll(client.getTagList());
                final RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
                final long vmStartTime = runtimeBean.getStartTime();
                final List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
                for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                    if (gcMXBean instanceof com.sun.management.GarbageCollectorMXBean) {
                        final GcInfo lastGcInfo = ((com.sun.management.GarbageCollectorMXBean) gcMXBean).getLastGcInfo();
    
                        // If no GCs, there was no pause.
                        if (lastGcInfo == null) {
                            continue;
                        }
    
                        final long gcStartTime = lastGcInfo.getStartTime() + vmStartTime;
                        if (gcStartTime > start) {
                            gcPause = true;
                            if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + lastGcInfo.getDuration() + " msec.");
                            break;
                        }
                    }
                }
                // redo the same op once more since there was a chance of gc pause
                if (gcPause) {
                    status = latch.await(to, unit);
                    tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.GC));
                    if (log.isDebugEnabled()) log.debug("Retry status : " + status);
                    if (status) {
                        tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.YES));
                    } else {
                        tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.NO));
                    }
                } else {
                    tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.SCHEDULE));
                }
                gcDuration = System.currentTimeMillis() - start;
                if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + (System.currentTimeMillis() - start) + " msec.");
            }
    
            for (Operation op : ops) {
                if (op.getState() != OperationState.COMPLETE) {
                    if (!status) {
                        MemcachedConnection.opTimedOut(op);
                        if(timedoutOps == null) timedoutOps = new HashSet<Operation>(); 
                        timedoutOps.add(op);
                    } else {
                        MemcachedConnection.opSucceeded(op);
                    }
                } else {
                    MemcachedConnection.opSucceeded(op);
                }
            }
    
            if (!status && !hasZF && (timedoutOps != null && timedoutOps.size() > 0)) statusString = EVCacheMetricsFactory.TIMEOUT;
    
            for (Operation op : ops) {
                if(op.isCancelled()) {
                    if (hasZF) statusString = EVCacheMetricsFactory.CANCELLED;
                    if (throwException) throw new ExecutionException(new CancellationException("Cancelled"));
                }
                if (op.hasErrored() && throwException) throw new ExecutionException(op.getException());
            }
            Map<String, T> m = new HashMap<String, T>();
            for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
                m.put(me.getKey(), me.getValue().get());
            }
            
            return m;
        } finally {
            if(gcDuration > 0) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.STATUS, statusString));
                EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_PAUSE, tagList, Duration.ofMillis(EVCacheConfig.getInstance().getChainedIntProperty(getApp() + ".max.read.duration.metric", "evcache.max.read.duration.metric", 20, null).get().intValue())).record(gcDuration, TimeUnit.MILLISECONDS);
            }
        }
    }

    public Single<Map<String, T>> observe() {
        return Single.create(subscriber ->
            addListener(future -> {
                try {
                    subscriber.onSuccess(get());
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            })
        );
    }

    public Single<Map<String, T>> getSome(long to, TimeUnit units, boolean throwException, boolean hasZF, Scheduler scheduler) {
        return observe().timeout(to, units, Single.create(subscriber -> {
            try {
                final Collection<Operation> timedoutOps = new HashSet<Operation>();
                for (Operation op : ops) {
                    if (op.getState() != OperationState.COMPLETE) {
                        MemcachedConnection.opTimedOut(op);
                        timedoutOps.add(op);
                    } else {
                        MemcachedConnection.opSucceeded(op);
                    }
                }

                //if (!hasZF && timedoutOps.size() > 0) EVCacheMetricsFactory.getInstance().increment(client.getAppName() + "-getSome-CheckedOperationTimeout", client.getTagList());

                for (Operation op : ops) {
                    if (op.isCancelled() && throwException) throw new ExecutionException(new CancellationException("Cancelled"));
                    if (op.hasErrored() && throwException) throw new ExecutionException(op.getException());
                }
                Map<String, T> m = new HashMap<String, T>();
                for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
                    m.put(me.getKey(), me.getValue().get());
                }
                subscriber.onSuccess(m);
            } catch (Throwable e) {
                subscriber.onError(e);
            }
        }), scheduler);
    }
    
    public String getZone() {
        return client.getServerGroupName();
    }

    public ServerGroup getServerGroup() {
        return client.getServerGroup();
    }

    public String getApp() {
        return client.getAppName();
    }

    public Set<String> getKeys() {
        return Collections.unmodifiableSet(rvMap.keySet());
    }

    public void signalComplete() {
        super.signalComplete();
    }

    public boolean cancel(boolean ign) {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
      return super.cancel(ign);
    }

    public long getStartTime() {
        return start;
    }
}