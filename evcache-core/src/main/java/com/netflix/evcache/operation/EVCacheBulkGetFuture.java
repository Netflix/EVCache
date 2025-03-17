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
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.util.Pair;
import net.spy.memcached.internal.BulkGetCompletionListener;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.ops.GetOperation;
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
    private AtomicReferenceArray<SingleOperationState> operationStates;

    public EVCacheBulkGetFuture(Map<String, Future<T>> m, Collection<Operation> getOps, CountDownLatch l, ExecutorService service, EVCacheClient client) {
        super(m, getOps, l, service);
        rvMap = m;
        ops = getOps;
        latch = l;
        this.start = System.currentTimeMillis();
        this.client = client;
        this.operationStates = null;
    }

    public Map<String, T> getSome(long to, TimeUnit unit, boolean throwException, boolean hasZF)
            throws InterruptedException, ExecutionException {
        assert operationStates != null;

        boolean allCompleted = latch.await(to, unit);
        if(log.isDebugEnabled()) log.debug("Took " + (System.currentTimeMillis() - start)+ " to fetch " + rvMap.size() + " keys from " + client);
        long pauseDuration = -1;
        List<Tag> tagList = null;
        String statusString = EVCacheMetricsFactory.SUCCESS;

        try {
            if (!allCompleted) {
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
                    allCompleted = latch.await(to, unit);
                    tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.GC));
                    if (log.isDebugEnabled()) log.debug("Retry status : " + allCompleted);
                    if (allCompleted) {
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

            boolean hadTimedoutOp = false;
            for (int i = 0; i < operationStates.length(); i++) {
                SingleOperationState state = operationStates.get(i);
                if (!state.completed && !allCompleted) {
                    MemcachedConnection.opTimedOut(state.op);
                    hadTimedoutOp = true;
                } else {
                    MemcachedConnection.opSucceeded(state.op);
                }
            }

            if (!allCompleted && !hasZF && hadTimedoutOp) statusString = EVCacheMetricsFactory.TIMEOUT;

            for (int i = 0; i < operationStates.length(); i++) {
                SingleOperationState state = operationStates.get(i);
                if (state.cancelled) {
                    if (hasZF) statusString = EVCacheMetricsFactory.CANCELLED;
                    if (throwException) throw new ExecutionException(new CancellationException("Cancelled"));
                }
                if (state.errored && throwException) throw new ExecutionException(state.op.getException());
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

    public void setExpectedCount(int size) {
        assert operationStates == null;

        operationStates = new AtomicReferenceArray<>(size);
    }

    // a lot of hoops we go through to avoid hitting the lock
    static class SingleOperationState {
        final Operation op;
        final boolean completed;
        final boolean cancelled;
        final boolean errored;
        final boolean timedOut;
        public SingleOperationState(Operation op) {
            this.op = op;
            this.completed = op.getState() == OperationState.COMPLETE;
            this.cancelled = op.isCancelled();
            this.errored = op.hasErrored();
            this.timedOut = op.isTimedOut();
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
        for (int i = 0; i < operationStates.length(); i++) {
            SingleOperationState state = operationStates.get(i);
            if (!state.completed) {
                if (state.cancelled) {
                    throw new RuntimeException(new ExecutionException(new CancellationException("Cancelled")));
                } else if (state.errored) {
                    throw new RuntimeException(new ExecutionException(state.op.getException()));
                } else {
                    state.op.timeOut();
                    MemcachedConnection.opTimedOut(state.op);
                    t = new ExecutionException(new CheckedOperationTimeoutException("Checked Operation timed out.", state.op));
                }
            } else {
                MemcachedConnection.opSucceeded(state.op);
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
                for (int i = 0; i < operationStates.length(); i++) {
                    SingleOperationState state = operationStates.get(i);
                    if (!state.completed) {
                        MemcachedConnection.opTimedOut(state.op);
                    } else {
                        MemcachedConnection.opSucceeded(state.op);
                    }
                }

                for (int i = 0; i < operationStates.length(); i++) {
                    SingleOperationState state = operationStates.get(i);
                    if (state.cancelled && throwException) throw new ExecutionException(new CancellationException("Cancelled"));
                    if (state.errored && throwException) throw new ExecutionException(state.op.getException());
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

    public void signalSingleOpComplete(int sequenceNo, GetOperation op) {
        this.operationStates.set(sequenceNo, new SingleOperationState(op));
    }

    public boolean cancel(boolean ign) {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
      return super.cancel(ign);
    }

    public long getStartTime() {
        return start;
    }
}
