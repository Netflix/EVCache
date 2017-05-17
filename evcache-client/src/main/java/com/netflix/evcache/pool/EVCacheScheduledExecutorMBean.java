package com.netflix.evcache.pool;

import java.util.concurrent.BlockingQueue;

public interface EVCacheScheduledExecutorMBean {

    boolean isShutdown();

    boolean isTerminating();

    boolean isTerminated();

    int getCorePoolSize();

    int getMaximumPoolSize();

    BlockingQueue<Runnable> getQueue();

    int getPoolSize();

    int getActiveCount();

    int getLargestPoolSize();

    long getTaskCount();

    long getCompletedTaskCount();

}