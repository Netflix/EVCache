package com.netflix.evcache;

import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.transcoders.Transcoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public interface EVCacheInternal extends EVCache {
    EVCacheItem<CachedData> metaGet(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException;

    Map<MemcachedNode, CachedValues> metaGetPerClient(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException;

    EVCacheItemMetaData metaDebug(String key, boolean isOriginalKeyHashed) throws EVCacheException;

    Map<MemcachedNode, EVCacheItemMetaData> metaDebugPerClient(String key, boolean isOriginalKeyHashed) throws EVCacheException;

    Future<Boolean>[] delete(String key, boolean isOriginalKeyHashed) throws EVCacheException;

    EVCacheLatch addOrSetToWriteOnly(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroup) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, List<String> destinationIps) throws EVCacheException;

    KeyHashedState isKeyHashed(String appName, String serverGroup);

    public enum KeyHashedState {
        YES,
        NO,
        MAYBE
    }

    public static class CachedValues {
        private final String key;
        private final CachedData data;
        private EVCacheItemMetaData itemMetaData;

        public CachedValues(String key, CachedData data, EVCacheItemMetaData itemMetaData) {
            this.key = key;
            this.data = data;
            this.itemMetaData = itemMetaData;
        }

        public String getKey() {
            return key;
        }

        public CachedData getData() {
            return data;
        }

        public EVCacheItemMetaData getEVCacheItemMetaData() {
            return itemMetaData;
        }


    }

    public class Builder extends EVCache.Builder {
        public Builder() {
            super();
        }

        @Override
        protected EVCache newImpl(String appName, String cachePrefix, int ttl, Transcoder<?> transcoder, boolean serverGroupRetry, boolean enableExceptionThrowing, EVCacheClientPoolManager poolManager) {
            return new EVCacheInternalImpl(appName, cachePrefix, ttl, transcoder, serverGroupRetry, enableExceptionThrowing, poolManager);
        }
    }

    public class BuilderInternal  extends EVCache.Builder {
        public BuilderInternal() {
            super();
        }
        protected EVCacheInternalImpl newImpl(String appName, String cachePrefix, int ttl, Transcoder<?> transcoder, boolean serverGroupRetry, boolean enableExceptionThrowing, EVCacheClientPoolManager poolManager) {
            return new EVCacheInternalImpl(appName, cachePrefix, ttl, transcoder, serverGroupRetry, enableExceptionThrowing, poolManager);
        }
    }
}