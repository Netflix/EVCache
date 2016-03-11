package com.netflix.evcache;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.MonitorConfig.Builder;
import com.netflix.servo.monitor.StepCounter;
import com.netflix.servo.tag.Tag;

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

    private void register(Monitor<?> monitor) {
        final MonitorRegistry registry = DefaultMonitorRegistry.getInstance();
        if (registry.isRegistered(monitor)) registry.unregister(monitor);
        // This will ensure old cache values are unregistered
        registry.register(monitor);
    }

    private MonitorConfig getMonitorConfig(String appName, String metric, Tag tag) {
        final Builder builder = MonitorConfig.builder("EVCacheInMemoryCache" + "-" + appName + "-" + metric).withTag(tag);
        return builder.build();
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

    private void setupMonitoring(final String appName) {
        final StepCounter sizeCounter = new StepCounter(getMonitorConfig(appName, "size", DataSourceType.GAUGE)) {
            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Long.valueOf(cache.size());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        register(sizeCounter);

        register(new Monitor<Number>() {
            final MonitorConfig config;

            {
                config = getMonitorConfig(appName, "requests", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Long.valueOf(cache.stats().requestCount());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }

            @Override
            public MonitorConfig getConfig() {
                return config;
            }
        });

        final StepCounter hitrateCounter = new StepCounter(getMonitorConfig(appName, "hitrate", DataSourceType.GAUGE)) {
            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Double.valueOf(cache.stats().hitRate());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        register(hitrateCounter);

        register(new Monitor<Number>() {
            final MonitorConfig config;

            {
                config = getMonitorConfig(appName, "hits", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Double.valueOf(cache.stats().hitCount());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }

            @Override
            public MonitorConfig getConfig() {
                return config;
            }
        });

        register(new Monitor<Number>() {
            final MonitorConfig config;

            {
                config = getMonitorConfig(appName, "miss", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Double.valueOf(cache.stats().missCount());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }

            @Override
            public MonitorConfig getConfig() {
                return config;
            }
        });

        register(new Monitor<Number>() {
            final MonitorConfig config;

            {
                config = getMonitorConfig(appName, "evictions", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (cache == null) return Long.valueOf(0);
                return Double.valueOf(cache.stats().evictionCount());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }

            @Override
            public MonitorConfig getConfig() {
                return config;
            }
        });
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
