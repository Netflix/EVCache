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
import java.util.concurrent.*;

import com.netflix.evcache.EVCacheGetOperationListener;
import net.spy.memcached.internal.BulkGetCompletionListener;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
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

    private static final Logger log = LoggerFactory.getLogger(EVCacheBulkGetFuture.class);
    private final Map<String, Future<T>> rvMap;
    private final Collection<Operation> ops;
    private final CountDownLatch latch;
    private final long start;
    private final EVCacheClient client;

    // as an optimization, we track the state of cancellation
    private volatile boolean receivedCancel = false;

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
        boolean allCompletedBeforeTimeout = latch.await(to, unit);
        if(log.isDebugEnabled()) log.debug("Took " + (System.currentTimeMillis() - start)+ " to fetch " + rvMap.size() + " keys from " + client);
        long pauseDuration = -1;
        List<Tag> tagList = null;
        String statusString = EVCacheMetricsFactory.SUCCESS;

        try {
            if (!allCompletedBeforeTimeout) {
                boolean gcPause = false;
                tagList = new ArrayList<Tag>(7);
                tagList.addAll(client.getTagList());
                    tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TAG, EVCacheMetricsFactory.BULK_OPERATION));
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
                    allCompletedBeforeTimeout = latch.await(to, unit);
                    tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.GC));
                    if (log.isDebugEnabled()) log.debug("Retry status : " + allCompletedBeforeTimeout);
                    if (allCompletedBeforeTimeout) {
                        tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.YES));
                    } else {
                        tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.NO));
                    }
                } else {
                    tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.SCHEDULE));
                }
                pauseDuration = System.currentTimeMillis() - start;
                if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + (System.currentTimeMillis() - start) + " msec.");
            }

            // Explicit short-circuits here to avoid op.getState() and op.getCancelled() most of the
            // time, which are `synchronized`. We aim to never need these in the typical/hot case.

            boolean hasTimedoutOps = false;
            for (Operation op : ops) {
                // short-circuited if is important to avoid expensive getState()
                if (!allCompletedBeforeTimeout && op.getState() != OperationState.COMPLETE) {
                    MemcachedConnection.opTimedOut(op);
                    hasTimedoutOps = true;
                } else {
                    MemcachedConnection.opSucceeded(op);
                }
            }

            if (hasTimedoutOps && !hasZF) {
                statusString = EVCacheMetricsFactory.TIMEOUT;
            }

            // Only need the cancel information in a subset of scenarios. We need to know firstly that, yes,
            // we received a cancel during processing, and that we need the result:
            // Specifically, we will either use the status in the finally block below, OR we plan to throw
            // an exception based on the results.
            boolean needsCancelCheck = receivedCancel && ((hasZF && pauseDuration > 0) || throwException);

            for (Operation op : ops) {
                // short-circuited if is important to avoid expensive isCancelled()
                if (needsCancelCheck && op.isCancelled()) {
                    if (hasZF) statusString = EVCacheMetricsFactory.CANCELLED;
                    if (throwException) throw new ExecutionException(new CancellationException("Cancelled"));
                }

                // interestingly enough, hasErrored is not synchronized
                if (op.hasErrored() && throwException) throw new ExecutionException(op.getException());
            }

            Map<String, T> m = new HashMap<String, T>();
            for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
                m.put(me.getKey(), me.getValue().get());
            }

            return m;
        } finally {
            if(pauseDuration > 0) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_STATUS, statusString));
                EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_PAUSE, tagList, Duration.ofMillis(EVCacheConfig.getInstance().getPropertyRepository().get(getApp() + ".max.read.duration.metric", Integer.class)
                        .orElseGet("evcache.max.read.duration.metric").orElse(20).get().intValue())).record(pauseDuration, TimeUnit.MILLISECONDS);
            }
        }
    }

    public CompletableFuture<Map<String, T>> getSomeCompletableFuture(long to, TimeUnit unit, boolean throwException, boolean hasZF) {
        CompletableFuture<Map<String, T>> completableFuture = new CompletableFuture<>();
       try {
           Map<String, T> value = getSome(to, unit, throwException, hasZF);
           completableFuture.complete(value);
       } catch (Exception e) {
            completableFuture.completeExceptionally(e);
       }
       return completableFuture;
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

    public <U> CompletableFuture<U> makeFutureWithTimeout(long timeout, TimeUnit units) {
        final CompletableFuture<U> future = new CompletableFuture<>();
        return EVCacheOperationFuture.withTimeout(future, timeout, units);
    }

    public CompletableFuture<Map<String, T>> getAsyncSome(long timeout, TimeUnit units) {
        CompletableFuture<Map<String, T>> future = makeFutureWithTimeout(timeout, units);
        doAsyncGetSome(future);
        return future.handle((data, ex) -> {
            if (ex != null) {
                handleBulkException();
            }
            return data;
        });
    }

    public void handleBulkException() {
        ExecutionException t = null;
        for (Operation op : ops) {
            if (op.getState() != OperationState.COMPLETE) {
                if (op.isCancelled()) {
                    throw new RuntimeException(new ExecutionException(new CancellationException("Cancelled")));
                }
                else if (op.hasErrored()) {
                    throw new RuntimeException(new ExecutionException(op.getException()));
                }
                else {
                    op.timeOut();
                    MemcachedConnection.opTimedOut(op);
                    t = new ExecutionException(new CheckedOperationTimeoutException("Checked Operation timed out.", op));
                }
            } else {
                MemcachedConnection.opSucceeded(op);
            }
        }
        throw new RuntimeException(t);
    }
    public void doAsyncGetSome(CompletableFuture<Map<String, T>> promise) {
        this.addListener(future -> {
            try {
                Map<String, T> m = new HashMap<>();
                Map<String, ?> result = future.get();
                for (Map.Entry<String, ?> me : result.entrySet()) {
                    m.put(me.getKey(), (T)me.getValue());
                }
                promise.complete(m);
            } catch (Exception t) {
                promise.completeExceptionally(t);
            }
        });
    }

    public Single<Map<String, T>> getSome(long to, TimeUnit units, boolean throwException, boolean hasZF, Scheduler scheduler) {
        return observe().timeout(to, units, Single.create(subscriber -> {
            try {
                for (Operation op : ops) {
                    if (op.getState() != OperationState.COMPLETE) {
                        MemcachedConnection.opTimedOut(op);
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


    public void setStatus(OperationStatus s) {
        if (s.getStatusCode() == StatusCode.CANCELLED) {
            receivedCancel = true;
        }

        super.setStatus(s);
    }
}
