package com.netflix.evcache.pool;

public interface EVCacheScheduledExecutorMBean {

    boolean isShutdown();

    boolean isTerminating();

    boolean isTerminated();

    int getCorePoolSize();

    int getMaximumPoolSize();

    int getQueueSize();

    int getPoolSize();

    int getActiveCount();

    int getLargestPoolSize();

    long getTaskCount();

    long getCompletedTaskCount();

}