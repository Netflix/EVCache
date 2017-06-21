package com.netflix.evcache.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.internal.ListenableFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

public class EVCacheLatchImpl implements EVCacheLatch, Runnable {
    private static final Logger log = LoggerFactory.getLogger(EVCacheLatchImpl.class);

    private final int expectedCompleteCount;
    private final CountDownLatch latch;
    private final List<Future<Boolean>> futures;
    private final Policy policy;
    private final int totalFutureCount;
    private final long start;

    private final String appName;

    private EVCacheEvent evcacheEvent = null;
    private boolean onCompleteDone = false;
    private int completeCount = 0;
    private int failureCount = 0;
    private ScheduledFuture<?> scheduledFuture;

    public EVCacheLatchImpl(Policy policy, int _count, String appName) {
        this.start = System.currentTimeMillis();
        this.policy = policy;
        this.futures = new ArrayList<Future<Boolean>>(_count);
        this.appName = appName;
        this.totalFutureCount = _count;
        this.expectedCompleteCount = policyToCount(policy, _count);
        this.latch = new CountDownLatch(expectedCompleteCount);

        if (log.isDebugEnabled()) log.debug("Number of Futures = " + _count + "; Number of Futures that need to completed for Latch to be released = " + this.expectedCompleteCount);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#await(long,java.util.concurrent.TimeUnit)
     */
    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (log.isDebugEnabled()) log.debug("Current Latch Count = " + latch.getCount() + "; await for "+ timeout + " " + unit.name() + " appName : " + appName);
        final long start = log.isDebugEnabled() ? System.currentTimeMillis() : 0;
        final boolean awaitSuccess = latch.await(timeout, unit);
        if (log.isDebugEnabled()) log.debug("await success = " + awaitSuccess + " after " + (System.currentTimeMillis() - start) + " msec." + " appName : " + appName + ((evcacheEvent != null) ? " keys : " + evcacheEvent.getCanonicalKeys() : ""));
        return awaitSuccess;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.evcache.operation.EVCacheLatchI#addFuture(net.spy.memcached.internal.ListenableFuture)
     */
    public void addFuture(ListenableFuture<Boolean, OperationCompletionListener> future) {
        future.addListener(this);
        if (future.isDone()) countDown();
        this.futures.add(future);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#isDone()
     */
    @Override
    public boolean isDone() {
        if (latch.getCount() == 0) return true;
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#countDown()
     */
    public void countDown() {
        if (log.isDebugEnabled()) log.debug("Current Latch Count = " + latch.getCount() + "; Count Down.");
        latch.countDown();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getPendingCount()
     */
    @Override
    public int getPendingCount() {
        if (log.isDebugEnabled()) log.debug("Pending Count = " + latch.getCount());
        return (int) latch.getCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getCompletedCount()
     */
    @Override
    public int getCompletedCount() {
        if (log.isDebugEnabled()) log.debug("Completed Count = " + completeCount);
        return completeCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getPendingFutures()
     */
    @Override
    public List<Future<Boolean>> getPendingFutures() {
        final List<Future<Boolean>> returnFutures = new ArrayList<Future<Boolean>>(expectedCompleteCount);
        for (Future<Boolean> future : futures) {
            if (!future.isDone()) {
                returnFutures.add(future);
            }
        }
        return returnFutures;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getAllFutures()
     */
    @Override
    public List<Future<Boolean>> getAllFutures() {
        return this.futures;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getCompletedFutures()
     */
    @Override
    public List<Future<Boolean>> getCompletedFutures() {
        final List<Future<Boolean>> returnFutures = new ArrayList<Future<Boolean>>(expectedCompleteCount);
        for (Future<Boolean> future : futures) {
            if (future.isDone()) {
                returnFutures.add(future);
            }
        }
        return returnFutures;
    }

    private int policyToCount(Policy policy, int count) {
        if (policy == null) return 0;
        switch (policy) {
        case NONE:
            return 0;
        case ONE:
            return 1;
        case QUORUM:
            if (count == 0)
                return 0;
            else if (count <= 2)
                return 1;
            else
                return (futures.size() / 2) + 1;
        case ALL_MINUS_1:
            if (count == 0)
                return 0;
            else if (count <= 2)
                return 1;
            else
                return count - 1;
        default:
            return count;
        }
    }
    
    public void setEVCacheEvent(EVCacheEvent e) {
        this.evcacheEvent = e;
    }

    /*
     * (non-Javadoc)
     * 
     * @see 
     * com.netflix.evcache.operation.EVCacheLatchI#onComplete(net.spy.memcached.internal.OperationFuture)
     */
    @Override
    public void onComplete(OperationFuture<?> future) throws Exception {
        if (log.isDebugEnabled()) log.debug("BEGIN : onComplete - Calling Countdown. Completed Future = " + future + "; App : " + appName); 
        countDown();
        completeCount++;
        if(evcacheEvent != null) {
            if (log.isDebugEnabled()) log.debug(";App : " + evcacheEvent.getAppName() + "; Call : " + evcacheEvent.getCall() + "; Keys : " + evcacheEvent.getCanonicalKeys() + "; completeCount : " + completeCount + "; totalFutureCount : " + totalFutureCount +"; failureCount : " + failureCount);
            if(future.isDone() && future.get().equals(Boolean.FALSE)) {
                failureCount++;
            }
            if(!onCompleteDone && getCompletedCount() >= getExpectedSuccessCount()) {
                if(evcacheEvent.getClients().size() > 0) {
                    for(EVCacheClient client : evcacheEvent.getClients()) {
                        final List<EVCacheEventListener> evcacheEventListenerList = client.getPool().getEVCacheClientPoolManager().getEVCacheEventListeners();
                        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
                            evcacheEventListener.onComplete(evcacheEvent);
                        }
                        onCompleteDone = true;//This ensures we fire onComplete only once
                        break;
                    }
                }
            }
            if(scheduledFuture != null) {
                final boolean futureCancelled = scheduledFuture.isCancelled(); 
                if (log.isDebugEnabled()) log.debug("App : " + evcacheEvent.getAppName() + "; Call : " + evcacheEvent.getCall() + "; Keys : " + evcacheEvent.getCanonicalKeys() + "; completeCount : " + completeCount + "; totalFutureCount : " + totalFutureCount +"; failureCount : " + failureCount + "; futureCancelled : " + futureCancelled);
                if(onCompleteDone && !futureCancelled) {
                    if(completeCount == totalFutureCount && failureCount == 0) { // all futures are completed
                        final boolean status = scheduledFuture.cancel(true);
                        if (log.isDebugEnabled()) log.debug("Cancelled the scheduled task : " + status);
                    }
                }
            }
            if (log.isDebugEnabled()) log.debug("App : " + evcacheEvent.getAppName() + "; Call : " + evcacheEvent.getCall() + "; Keys : " + evcacheEvent.getCanonicalKeys() + "; completeCount : " + completeCount + "; totalFutureCount : " + totalFutureCount +"; failureCount : " + failureCount);
        }
        if(totalFutureCount == completeCount) {
            final List<Tag> tags = new ArrayList<Tag>(3);
            tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
            tags.add(new BasicTag(EVCacheMetricsFactory.FAIL_COUNT, String.valueOf(failureCount)));
            tags.add(new BasicTag(EVCacheMetricsFactory.COMPLETE_COUNT, String.valueOf(completeCount)));
            EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_LATCH_CALLBACK, tags).record(System.currentTimeMillis()- start, TimeUnit.MILLISECONDS);
        }
        if (log.isDebugEnabled()) log.debug("END : onComplete - Calling Countdown. Completed Future = " + future + "; App : " + appName); 
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getFailureCount()
     */
    @Override
    public int getFailureCount() {
        int fail = 0;
        for (Future<Boolean> future : futures) {
            try {
                if (future.isDone() && future.get().equals(Boolean.FALSE)) {
                    fail++;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return fail;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.evcache.operation.EVCacheLatchI#getExpectedCompleteCount()
     */
    @Override
    public int getExpectedCompleteCount() {
        return this.expectedCompleteCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.evcache.operation.EVCacheLatchI#getExpectedSuccessCount()
     */
    @Override
    public int getExpectedSuccessCount() {
        return this.expectedCompleteCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.netflix.evcache.operation.EVCacheLatchI#getSuccessCount()
     */
    @Override
    public int getSuccessCount() {
        int success = 0;
        for (Future<Boolean> future : futures) {
            try {
                if (future.isDone() && future.get().equals(Boolean.TRUE)) {
                    success++;
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return success;
    }

    public String getAppName() {
        return appName;
    }

    public Policy getPolicy() {
        return this.policy;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"AppName\":\"");
        builder.append(getAppName());
        builder.append("\",\"isDone\":\"");
        builder.append(isDone());
        builder.append("\",\"Pending Count\":\"");
        builder.append(getPendingCount());
        builder.append("\",\"Completed Count\":\"");
        builder.append(getCompletedCount());
        builder.append("\",\"Pending Futures\":\"");
        builder.append(getPendingFutures());
        builder.append("\",\"All Futures\":\"");
        builder.append(getAllFutures());
        builder.append("\",\"Completed Futures\":\"");
        builder.append(getCompletedFutures());
        builder.append("\",\"Failure Count\":\"");
        builder.append(getFailureCount());
        builder.append("\",\"Success Count\":\"");
        builder.append(getSuccessCount());
        builder.append("\",\"Excpected Success Count\":\"");
        builder.append(getExpectedSuccessCount());
        builder.append("\"}");
        return builder.toString();
    }

    @Override
    public int getPendingFutureCount() {
        int count = 0;
        for (Future<Boolean> future : futures) {
            if (!future.isDone()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int getCompletedFutureCount() {
        int count = 0;
        for (Future<Boolean> future : futures) {
            if (future.isDone()) {
                count++;
            }
        }
        return count;
    }
    
    public boolean isFastFailure() {
        return (totalFutureCount == 0); 
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));

        if(evcacheEvent != null) {
            int failCount = 0;

            for (Future<Boolean> future : futures) {
                boolean fail = false;
                try {
                    fail = future.get().equals(Boolean.FALSE);
                } catch (Exception e) {
                    fail = true;
                    if(log.isDebugEnabled()) log.debug(e.getMessage(), e);
                }

                if (fail) {
                    failCount++;
                    if(future instanceof EVCacheOperationFuture) {
                        final EVCacheOperationFuture<Boolean> evcFuture = (EVCacheOperationFuture<Boolean>)future;
                        List<ServerGroup> listOfFailedServerGroups = (List<ServerGroup>) evcacheEvent.getAttribute("FailedServerGroups");
                        if(listOfFailedServerGroups == null) {
                            listOfFailedServerGroups = new ArrayList<ServerGroup>(failCount);
                            evcacheEvent.setAttribute("FailedServerGroups", listOfFailedServerGroups);
                        }
                        listOfFailedServerGroups.add(evcFuture.getServerGroup());
                        tags.add(new BasicTag(EVCacheMetricsFactory.FAILED_SERVERGROUP, evcFuture.getServerGroup().getName()));
                    }
                }
            }
            if(log.isDebugEnabled()) log.debug("Fail Count : " + failCount);
            tags.add(new BasicTag(EVCacheMetricsFactory.FAIL_COUNT, String.valueOf(failCount)));

            if(failCount > 0) {
                if(evcacheEvent.getClients().size() > 0) {
                    for(EVCacheClient client : evcacheEvent.getClients()) {
                        final List<EVCacheEventListener> evcacheEventListenerList = client.getPool().getEVCacheClientPoolManager().getEVCacheEventListeners();
                        if(log.isDebugEnabled()) log.debug("\nClient : " + client +"\nEvcacheEventListenerList : " + evcacheEventListenerList);
                        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
                            evcacheEventListener.onError(evcacheEvent, null);
                        }
                        break;
                    }
                }
            } 
        }
        EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.INTERNAL_LATCH_VERIFY, tags).increment();
    }

    @Override
    public int hashCode() {
        return ((evcacheEvent == null) ? 0 : evcacheEvent.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVCacheLatchImpl other = (EVCacheLatchImpl) obj;
        if (appName == null) {
            if (other.appName != null)
                return false;
        } else if (!appName.equals(other.appName))
            return false;
        if (evcacheEvent == null) {
            if (other.evcacheEvent != null)
                return false;
        } else if (!evcacheEvent.equals(other.evcacheEvent))
            return false;
        return true;
    }

    public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        this.scheduledFuture = scheduledFuture;
    }

}