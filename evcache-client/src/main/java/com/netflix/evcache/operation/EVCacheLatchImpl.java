package com.netflix.evcache.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;

import net.spy.memcached.internal.ListenableFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

public class EVCacheLatchImpl implements EVCacheLatch {
    private static final Logger log = LoggerFactory.getLogger(EVCacheLatchImpl.class);

    private final int expectedCompleteCount;
    private final CountDownLatch latch;
    private final List<Future<Boolean>> futures;
    private final Policy policy;
    private final int totalFutureCount;

    private final String appName;

    private EVCacheEvent evcacheEvent = null;

    public EVCacheLatchImpl(Policy policy, int _count, String appName) {
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
     * @see com.netflix.evcache.operation.EVCacheLatchI#await(long,
     * java.util.concurrent.TimeUnit)
     */
    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (log.isDebugEnabled()) log.debug("Current Latch Count = " + latch.getCount() + "; Will Start the wait");
        return latch.await(timeout, unit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.netflix.evcache.operation.EVCacheLatchI#addFuture(net.spy.memcached.
     * internal.ListenableFuture)
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
        final int completedCount = (totalFutureCount - (int) latch.getCount());
        if (log.isDebugEnabled()) log.debug("Completed Count = " + completedCount);
        return completedCount;
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
     * com.netflix.evcache.operation.EVCacheLatchI#onComplete(net.spy.memcached.
     * internal.OperationFuture)
     */
    @Override
    public void onComplete(OperationFuture<?> future) throws Exception {
        if (log.isDebugEnabled()) log.debug("onComplete Callback. Calling Countdown. Completed Future = " + future);
        countDown();
        if(evcacheEvent != null) { 
            if(getCompletedCount() >= getExpectedSuccessCount()) {
                if(evcacheEvent.getClients().size() > 0) {
                    for(EVCacheClient client : evcacheEvent.getClients()) {
                        final List<EVCacheEventListener> evcacheEventListenerList = client.getPool().getEVCacheClientPoolManager().getEVCacheEventListeners();
                        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
                            evcacheEventListener.onComplete(evcacheEvent);
                        }
                        break;
                    }
                }
            }
            final int failCount = getFailureCount();
            if(getPendingFutureCount() == 0 && failCount > 0) {
                if(evcacheEvent.getClients().size() > 0) {
                    for (Future<Boolean> _future : futures) {
                        try {
                            if (_future.isDone() && _future.get().equals(Boolean.FALSE)) {
                                if(_future instanceof EVCacheOperationFuture) {
                                    final EVCacheOperationFuture<Boolean> evcFuture = (EVCacheOperationFuture<Boolean>)_future;
                                    List<ServerGroup> listOfFailedServerGroups = (List<ServerGroup>) evcacheEvent.getAttribute("FailedServerGroups");
                                    if(listOfFailedServerGroups == null) {
                                        listOfFailedServerGroups = new ArrayList<ServerGroup>(failCount);
                                        evcacheEvent.setAttribute("FailedServerGroups", listOfFailedServerGroups);
                                    }
                                    listOfFailedServerGroups.add(evcFuture.getServerGroup());
                                }
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    for(EVCacheClient client : evcacheEvent.getClients()) {
                        final List<EVCacheEventListener> evcacheEventListenerList = client.getPool().getEVCacheClientPoolManager().getEVCacheEventListeners();
                        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
                            evcacheEventListener.onError(evcacheEvent, null);
                        }
                        break;
                    }

                }
            }
        }
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

}