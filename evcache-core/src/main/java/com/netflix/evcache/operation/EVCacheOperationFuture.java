package com.netflix.evcache.operation;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import com.sun.management.GcInfo;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.Operation;
import rx.Scheduler;
import rx.Single;
import rx.functions.Action0;

/**
 * Managed future for operations.
 *
 * <p>
 * From an OperationFuture, application code can determine if the status of a
 * given Operation in an asynchronous manner.
 *
 * <p>
 * If for example we needed to update the keys "user:<userid>:name",
 * "user:<userid>:friendlist" because later in the method we were going to
 * verify the change occurred as expected interacting with the user, we can fire
 * multiple IO operations simultaneously with this concept.
 *
 * @param <T>
 *            Type of object returned from this future.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_HAS_CHECKED")
public class EVCacheOperationFuture<T> extends OperationFuture<T> {

    private Logger log = LoggerFactory.getLogger(EVCacheOperationFuture.class);

    private final CountDownLatch latch;
    private final AtomicReference<T> objRef;
    private Operation op;
    private final String key;
    private final long start;
    private final EVCacheClient client;

    public EVCacheOperationFuture(String k, CountDownLatch l, AtomicReference<T> oref, long opTimeout, ExecutorService service, EVCacheClient client) {
        super(k, l, oref, opTimeout, service);
        this.latch = l;
        this.objRef = oref;
        this.key = k;
        this.client = client;
        this.start = System.currentTimeMillis();
    }

    public Operation getOperation() {
        return this.op;
    }

    public void setOperation(Operation to) {
        this.op = to;
        super.setOperation(to);
    }

    public String getApp() {
        return client.getAppName();
    }

    public String getKey() {
        return key;
    }

    public String getZone() {
        return client.getZone();
    }

    public ServerGroup getServerGroup() {
        return client.getServerGroup();
    }

    public EVCacheClient getEVCacheClient() {
        return client;
    }

    public EVCacheOperationFuture<T> addListener(EVCacheGetOperationListener<T> listener) {
        super.addToListeners(listener);
        return this;
    }

    public EVCacheOperationFuture<T> removeListener(EVCacheGetOperationListener<T> listener) {
        super.removeFromListeners(listener);
        return this;
    }

    /**
     * Get the results of the given operation.
     *
     * As with the Future interface, this call will block until the results of
     * the future operation has been received.
     * 
     * Note: If we detect there was GC pause and our operation was caught in
     * between we wait again to see if we will be successful. This is effective
     * as the timeout we specify is very low.
     *
     * @param duration
     *            amount of time to wait
     * @param units
     *            unit of time to wait
     * @param if
     *            exeception needs to be thrown of null returned on a failed
     *            operation
     * @param has
     *            zone fallback
     * @return the operation results of this OperationFuture
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    public T get(long duration, TimeUnit units, boolean throwException, boolean hasZF) throws InterruptedException, TimeoutException, ExecutionException {
        //final long startTime = System.currentTimeMillis();
        boolean status = latch.await(duration, units);
        if(log.isDebugEnabled()) log.debug("Took " + (System.currentTimeMillis() - start)+ " to fetch key " + key + " from " + client);
        long gcDuration = -1;
        List<Tag> tagList = null;
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
                            gcDuration = System.currentTimeMillis() - start;
                            if (log.isDebugEnabled()) log.debug("Total duration due to gc event = " + gcDuration + " msec.");
                            break;
                        }
                    }
                }
                if (!gcPause) 
                    // redo the same op once more since there was a chance of gc pause
                    if (gcPause) {
                        status = latch.await(duration, units);
                        tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.GC));
                        if (log.isDebugEnabled()) log.debug("Retry status : " + status);

                        if (status) {
                            tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.YES));
                        } else {
                            tagList.add(new BasicTag(EVCacheMetricsFactory.FETCH_AFTER_PAUSE, EVCacheMetricsFactory.NO));
                        }
                    } else {
                        gcDuration = System.currentTimeMillis() - start;
                        gcPause = (gcDuration > units.toMillis(duration) + 10);
                        if (gcPause) {
                            tagList.add(new BasicTag(EVCacheMetricsFactory.PAUSE_REASON, EVCacheMetricsFactory.UNKNOWN));
                        }
                    }
            }

            if (!status) {
                // whenever timeout occurs, continuous timeout counter will increase by 1.
                MemcachedConnection.opTimedOut(op);
                if (op != null) op.timeOut();
                if (!hasZF) statusString = EVCacheMetricsFactory.TIMEOUT;
                if (throwException) {
                    throw new CheckedOperationTimeoutException("Timed out waiting for operation", op);
                }
            } else {
                // continuous timeout counter will be reset
                MemcachedConnection.opSucceeded(op);
            }

            if (op != null && op.hasErrored()) {
                if (throwException) {
                    throw new ExecutionException(op.getException());
                }
            }
            if (isCancelled()) {
                if (hasZF) statusString = EVCacheMetricsFactory.CANCELLED;
                if (throwException) {
                    throw new ExecutionException(new CancellationException("Cancelled"));
                }
            }
            if (op != null && op.isTimedOut()) {
                if (throwException) {
                    throw new ExecutionException(new CheckedOperationTimeoutException("Operation timed out.", op));
                }
            }
            return objRef.get();
        } finally {
            if(gcDuration > 0) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.STATUS, statusString));
                EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_PAUSE, tagList).record(gcDuration, TimeUnit.MILLISECONDS);
            }
        }
    }

    public Single<T> observe() {
        return Single.create(subscriber ->
            addListener((EVCacheGetOperationListener<T>) future -> {
                try {
                    subscriber.onSuccess(get());
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            })
        );
    }

    public Single<T> get(long duration, TimeUnit units, boolean throwException, boolean hasZF, Scheduler scheduler) {
        return observe().timeout(duration, units, Single.create(subscriber -> {
            // whenever timeout occurs, continuous timeout counter will increase by 1.
            MemcachedConnection.opTimedOut(op);
            if (op != null) op.timeOut();
            //if (!hasZF) EVCacheMetricsFactory.getInstance().increment(getApp() + "-get-CheckedOperationTimeout", client.getTagList());
            if (throwException) {
                subscriber.onError(new CheckedOperationTimeoutException("Timed out waiting for operation", op));
            } else {
                if (isCancelled()) {
                    //if (hasZF) EVCacheMetricsFactory.getInstance().increment(getApp()+ "-get-Cancelled", client.getTagList());
                }
                subscriber.onSuccess(objRef.get());
            }
        }), scheduler).doAfterTerminate(new Action0() {
            @Override
            public void call() {
                
            }
        }
        );
    }

    public void signalComplete() {
        super.signalComplete();
    }
    
    /**
     * Cancel this operation, if possible.
     *
     * @param ign not used
     * @deprecated
     * @return true if the operation has not yet been written to the network
     */
    public boolean cancel(boolean ign) {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
      return super.cancel(ign);
    }

    /**
     * Cancel this operation, if possible.
     *
     * @return true if the operation has not yet been written to the network
     */
    public boolean cancel() {
        if(log.isDebugEnabled()) log.debug("Operation cancelled", new Exception());
        return super.cancel();
    }

    public long getStartTime() {
        return start;
    }

}