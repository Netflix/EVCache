package com.netflix.evcache.operation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
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

import com.sun.management.GcInfo;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.Stopwatch;
import rx.Scheduler;
import rx.Single;

/**
 * Future for handling results from bulk gets.
 *
 * Not intended for general use.
 *
 * types of objects returned from the GETBULK
 */
public class EVCacheBulkGetFuture<T> extends BulkGetFuture<T> {

    private Logger log = LoggerFactory.getLogger(EVCacheBulkGetFuture.class);
    private final Map<String, Future<T>> rvMap;
    private final Collection<Operation> ops;
    private final CountDownLatch latch;
    private final String appName;
    private final ServerGroup serverGroup;
    private final String metricName;

    public EVCacheBulkGetFuture(String appName, Map<String, Future<T>> m, Collection<Operation> getOps, CountDownLatch l, ExecutorService service, ServerGroup serverGroup, String metricName) {
        super(m, getOps, l, service);
        this.appName = appName;
        rvMap = m;
        ops = getOps;
        latch = l;
        this.serverGroup = serverGroup;
        this.metricName = metricName;
    }

    public Map<String, T> getSome(long to, TimeUnit unit, boolean throwException, boolean hasZF)
            throws InterruptedException, ExecutionException {
        final Collection<Operation> timedoutOps = new HashSet<Operation>();

        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, metricName).start();
        final long startTime = System.currentTimeMillis();
        boolean status = latch.await(to, unit);

        if (!status) {
            boolean gcPause = false;
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
                    if (gcStartTime > startTime) {
                        gcPause = true;
                        final long gcDuration = lastGcInfo.getDuration();
                        EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-DelayDueToGCPause", DataSourceType.COUNTER).increment(gcDuration);
                        if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + gcDuration
                                + " msec.");
                        break;
                    }
                }
            }
            if (!gcPause) {
                long gcDuration = System.currentTimeMillis() - startTime;
                EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-DelayProbablyDueToGCPause", DataSourceType.COUNTER).increment(gcDuration);
            }
            // redo the same op once more since there was a chance of gc pause
            if (gcPause) {
                status = latch.await(to, unit);
                if (log.isDebugEnabled()) log.debug("Retry status : " + status);
                if (status) {
                    EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-DelayDueToGCPause-Success", DataSourceType.COUNTER).increment();
                } else {
                    EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-DelayDueToGCPause-Fail", DataSourceType.COUNTER).increment();
                }
            }
            if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + (System.currentTimeMillis()
                    - startTime) + " msec.");
        }

        for (Operation op : ops) {
            if (op.getState() != OperationState.COMPLETE) {
                if (!status) {
                    MemcachedConnection.opTimedOut(op);
                    timedoutOps.add(op);
                } else {
                    MemcachedConnection.opSucceeded(op);
                }
            } else {
                MemcachedConnection.opSucceeded(op);
            }
        }

        if (!status && !hasZF && timedoutOps.size() > 0) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-getSome-CheckedOperationTimeout", DataSourceType.COUNTER).increment();

        for (Operation op : ops) {
            if(op.isCancelled()) {
                if (hasZF) EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-getSome-Cancelled", DataSourceType.COUNTER).increment();
                if (throwException) throw new ExecutionException(new CancellationException("Cancelled"));
            }
            if (op.hasErrored() && throwException) throw new ExecutionException(op.getException());
        }
        Map<String, T> m = new HashMap<String, T>();
        for (Map.Entry<String, Future<T>> me : rvMap.entrySet()) {
            m.put(me.getKey(), me.getValue().get());
        }
        operationDuration.stop();
        return m;
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
        final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, metricName).start();
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

                if (!hasZF && timedoutOps.size() > 0) EVCacheMetricsFactory.increment(appName + "-getSome-CheckedOperationTimeout");

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
        }), scheduler).doAfterTerminate(() ->
            operationDuration.stop()
        );
    }
    
    public String getZone() {
        return (serverGroup == null ? "NA" : serverGroup.getZone());
    }

    public ServerGroup getServerGroup() {
        return serverGroup;
    }

    public String getApp() {
        return appName;
    }

    public Set<String> getKeys() {
        return Collections.unmodifiableSet(rvMap.keySet());
    }

    public void signalComplete() {
        super.signalComplete();
    }

}