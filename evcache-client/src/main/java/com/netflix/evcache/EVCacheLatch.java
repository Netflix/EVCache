package com.netflix.evcache;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.OperationCompletionListener;

public interface EVCacheLatch extends OperationCompletionListener {

    /**
     * The Policy which can be used to control the latch behavior. The latch is released when the number operations as specified by the Policy are completed.
     * For example: If your evcache app has 3 copies (3 server groups) in a region then each write done on that app will perform 3 operations (one for each copy/server group).
     * If you are doing a set operation and the selected Policy is ALL_MINUS_1 then we need to complete 2 operations(set on 2 copies/server groups) need to be finished before we release the latch. 
     *
     * Note that an Operation completed means that the operation was accepted by evcache or rejected by evcache.
     * If it is still in flight the that operation is in pending state. 
     *
     * Case ALL : All the operations have to be completed.
     * Case All_MINUS_1 : All but one needs to be completed. For ex: If there are 3 copies for a cache then 2 need to be completed.
     * Case QUORUM: Quorum number of operations have to be completed before we release the latch: for a cluster with 3 this means 2 operations need to be completed. 
     * Case ONE: At least one operations needs to be completed before we release the latch.
     * Case NONE: The latch is released immediately. 
     * 
     * @author smadappa
     *
     */
    public static enum Policy {
        NONE, ONE, QUORUM, ALL_MINUS_1, ALL
    }

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is interrupted, or the specified waiting time
     * elapses.
     * 
     * @param timeout
     *            - the maximum time to wait
     * @param unit
     *            - the time unit of the timeout argument
     * 
     * @return - {@code true} if the count reached zero and false if the waiting
     *         time elapsed before the count reached zero
     * @throws InterruptedException
     *             if the current thread is interrupted while waiting
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns {@code true} if this all the tasks assigned for this Latch
     * completed.
     *
     * Completion may be due to normal termination, an exception, or
     * cancellation -- in all of these cases, this method will return
     * {@code true}.
     *
     * @return {@code true} if all the tasks completed
     */
    boolean isDone();

    /**
     * Returns the Futures backing the Pending tasks.
     *
     * @return the current outstanding tasks
     */
    List<Future<Boolean>> getPendingFutures();

    /**
     * Returns all the Tasks.
     *
     * @return the tasks submitted part of this Latch
     */
    List<Future<Boolean>> getAllFutures();

    /**
     * Returns all the completed Tasks.
     *
     * @return the current completed tasks
     */
    List<Future<Boolean>> getCompletedFutures();

    /**
     * Returns the number of Tasks that are still Pending.
     *
     * @return the current outstanding task count
     */
    int getPendingCount();

    /**
     * Returns the number of Tasks that are completed. A task is completed if
     * the task was finished either success of failure. The task is considered
     * failure if it times out or there was an exception.
     *
     * @return the completed task count
     */
    int getCompletedCount();

    /**
     * Returns the number of Tasks that failed to complete. There was either an
     * exception or the task was cancelled.
     *
     * @return the failed task count
     */
    int getFailureCount();

    /**
     * Returns the number of Tasks that need to be successfully completed based
     * on the Specified Policy before the latch can be released.
     *
     * @return the expected success count
     */
    int getExpectedSuccessCount();

    /**
     * Returns the current number of Tasks that are successful .
     *
     * @return the current Successful Task count.
     */
    int getSuccessCount();

    /**
     * The {@code Policy} for this Latch
     *
     * @return the Latch.
     */
    Policy getPolicy();
}