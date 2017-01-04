package net.spy.memcached.protocol.binary;

public interface EVCacheNodeImplMBean {

    int getContinuousTimeout();

    int getReconnectCount();

    boolean isActive();

    int getWriteQueueSize();

    int getReadQueueSize();

    int getInputQueueSize();

    long getNumOfOps();

    void flushInputQueue();

    void removeMonitoring();
}