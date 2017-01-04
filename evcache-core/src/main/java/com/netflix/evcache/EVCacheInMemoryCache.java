package com.netflix.evcache;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.transcoders.Transcoder;

/**
 * An In Memory cache that can be used to hold data for short duration. This is
 * helpful when the same key is repeatedly requested from EVCache within a short
 * duration. This can be turned on dynamically and can relive pressure on
 * EVCache Server instances.
 */
public class EVCacheInMemoryCache<T> {

    private static final Logger log = LoggerFactory.getLogger(EVCacheInMemoryCache.class);
    private final DynamicIntProperty _cacheDuration; // The key will be cached for this long
    private final DynamicIntProperty _cacheSize; // This many items will be cached
//    private final DynamicLongProperty _cacheWeight; // This is the max size in bytes 
    private final String appName;

    private Cache<String, T> cache;

    public EVCacheInMemoryCache(String appName) {
        this.appName = appName;
        this._cacheDuration = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.cache.duration.ms", 20);
        this._cacheDuration.addCallback(new Runnable() {
            public void run() {
                setupCache();
            }
        });

        this._cacheSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.cache.size", 100);
//        this._cacheWeight = EVCacheConfig.getInstance().getDynamicLongProperty(appName + ".inmemory.cache.weight", 0); //
        this._cacheSize.addCallback(new Runnable() {
            public void run() {
                setupCache();
            }
        });
        setupCache();
    }

    private void setupCache() {
        try {
            final Cache<String, T> currentCache = this.cache;
            CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().recordStats();
            if(_cacheSize.get() > 0) {
                builder = builder.maximumSize(_cacheSize.get());
            }
            if(_cacheDuration.get() > 0) {
                builder = builder.expireAfterWrite(_cacheDuration.get(), TimeUnit.MILLISECONDS);
            }
//            if(_cacheWeight.get() > 0) {
//                builder = builder.maximumWeight(_cacheWeight.get());
//            }
            setupMonitoring(appName);
            this.cache = builder.build();
            if(currentCache != null) {
                currentCache.invalidateAll();
                currentCache.cleanUp();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    private long getSize() {
        final long size = cache.size();
        final CacheStats stats = cache.stats();
        EVCacheMetricsFactory.getInstance().getRegistry().counter("EVCacheInMemoryCache-" + appName + "-hits").increment(stats.hitCount());
        EVCacheMetricsFactory.getInstance().getRegistry().counter("EVCacheInMemoryCache-" + appName + "-miss").increment(stats.missCount());
        EVCacheMetricsFactory.getInstance().getRegistry().counter("EVCacheInMemoryCache-" + appName + "-evictions").increment(stats.evictionCount());
        EVCacheMetricsFactory.getInstance().getRegistry().counter("EVCacheInMemoryCache-" + appName + "-requests").increment(stats.requestCount());
        EVCacheMetricsFactory.getInstance().getRegistry().gauge("EVCacheInMemoryCache-" + appName + "-hitrate", stats, CacheStats::hitRate);
        return size;
    }

    private void setupMonitoring(final String appName) {
        EVCacheMetricsFactory.getInstance().getRegistry().gauge("EVCacheInMemoryCache" + "-" + appName + "-size", this, EVCacheInMemoryCache::getSize);
    }

    public T get(String key) {
        if (cache == null) return null;
        final T val = cache.getIfPresent(key);
        if (log.isDebugEnabled()) log.debug("GET : appName : " + appName + "; Key : " + key + "; val : " + val);
        return val;
    }

    public void put(String key, T value) {
        if (cache == null) return;
        cache.put(key, value);
        if (log.isDebugEnabled()) log.debug("PUT : appName : " + appName + "; Key : " + key + "; val : " + value);
    }

    public void delete(String key) {
        if (cache == null) return;
        cache.invalidate(key);
        if (log.isDebugEnabled()) log.debug("DEL : appName : " + appName + "; Key : " + key);
    }
    
    static class DataWeigher implements Weigher<Object, Object> {
        
        private Transcoder<?> transcoder;
        
        DataWeigher() {
            
        }

        public Transcoder<?> getTranscoder() {
            return transcoder;
        }

        public void setTranscoder(Transcoder<?> transcoder) {
            this.transcoder = transcoder;
        }

        public int weigh(Object key, Object value) {
            // TODO Auto-generated method stub
            return 0;
        }
        
    }
}
