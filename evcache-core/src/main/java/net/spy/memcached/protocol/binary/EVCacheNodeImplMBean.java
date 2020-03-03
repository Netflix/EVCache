package net.spy.memcached.protocol.binary;

public interface EVCacheNodeImplMBean {

    int getContinuousTimeout();

    int getReconnectCount();

    boolean isActive();

    int getWriteQueueSize();

    int getReadQueueSize();

    int getInputQueueSize();

    long getNumOfOps();

    String getSocketChannelLocalAddress();
    
    String getSocketChannelRemoteAddress();
    
    String getConnectTime();

    void flushInputQueue();

    void removeMonitoring();
        
}