package net.spy.memcached;

import java.util.List;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.spectator.api.Tag;

public interface EVCacheNode extends MemcachedNode {
    void registerMonitors();

    boolean isAvailable(EVCache.Call call);

    int getWriteQueueSize();

    int getReadQueueSize();

    int getInputQueueSize();

    long incrOps();

    long getNumOfOps();

    void flushInputQueue();

    long getStartTime();

    long getTimeoutStartTime();

    void removeMonitoring();

    void shutdown();

    long getCreateTime();

    void setConnectTime(long cTime);

    String getAppName();

    String getHostName();

    ServerGroup getServerGroup();

    int getId();

    List<Tag> getTags();

    int getTotalReconnectCount();

    String getSocketChannelLocalAddress();

    String getSocketChannelRemoteAddress();

    String getConnectTime();

    int getContinuousTimeout();

    int getReconnectCount();

    boolean isActive();
    
    EVCacheClient getEVCacheClient();

}