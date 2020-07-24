package com.netflix.evcache;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;

public interface EVCacheInternal extends EVCache {
    EVCacheItem<CachedData> metaGet(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException;

    EVCacheItemMetaData metaDebug(String key, boolean isOriginalKeyHashed) throws EVCacheException;

    Future<Boolean>[] delete(String key, boolean isOriginalKeyHashed) throws EVCacheException;

    EVCacheLatch addOrSetToWriteOnly(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroup) throws EVCacheException;

    EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, String destinationIp) throws EVCacheException;

    KeyHashedState isKeyHashed(String appName, String serverGroup);

    public enum KeyHashedState {
        YES,
        NO,
        MAYBE
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
}