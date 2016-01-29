package com.netflix.evcache;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.OperationCompletionListener;

public interface EVCacheLatch extends OperationCompletionListener {

    public static enum Policy {
        ONE, QUORUM, ALL_MINUS_1, ALL
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