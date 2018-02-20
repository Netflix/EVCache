package com.netflix.evcache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.transcoders.Transcoder;

/**
 * An In Memory cache that can be used to hold data for short duration. This is
 * helpful when the same key is repeatedly requested from EVCache within a short
 * duration. This can be turned on dynamically and can relive pressure on
 * EVCache Server instances.
 */
public class EVCacheInMemoryCache<T> {

    private static final Logger log = LoggerFactory.getLogger(EVCacheInMemoryCache.class);
    private final ChainedDynamicProperty.IntProperty _cacheDuration; // The key will be cached for this long
    private final DynamicIntProperty _refreshDuration, _exireAfterAccessDuration;
    private final DynamicIntProperty _cacheSize; // This many items will be cached
    private final DynamicIntProperty _poolSize; // This many threads will be initialized to fetch data from evcache async
    private final String appName;
    private final Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
    private final Map<String, Gauge> gaugeMap = new ConcurrentHashMap<String, Gauge>();

    private LoadingCache<String, T> cache;
    private ExecutorService pool = null;

    private final Transcoder<T> tc;
    private final EVCacheImpl impl;
    private final Id sizeId;

    public EVCacheInMemoryCache(String appName, Transcoder<T> tc, EVCacheImpl impl) {
        this.appName = appName;
        this.tc = tc;
        this.impl = impl;
        final Runnable callback = new Runnable() {
            public void run() {
                setupCache();
            }
        };

        this._cacheDuration = EVCacheConfig.getInstance().getChainedIntProperty(appName + ".inmemory.expire.after.write.duration.ms", appName + ".inmemory.cache.duration.ms", 0, callback);

        this._exireAfterAccessDuration = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.expire.after.access.duration.ms", 0);
        this._exireAfterAccessDuration.addCallback(callback);

        this._refreshDuration = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.refresh.after.write.duration.ms", 0);
        this._refreshDuration.addCallback(callback);

        this._cacheSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.cache.size", 100);
        this._cacheSize.addCallback(callback);

        this._poolSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".thread.pool.size", 5);
        this._poolSize.addCallback(new Runnable() {
            public void run() {
                initRefreshPool();
            }
        });
        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.METRIC, "size"));

        this.sizeId = EVCacheMetricsFactory.getInstance().getId(EVCacheMetricsFactory.IN_MEMORY, tags);
        setupCache();
        setupMonitoring(appName);
    }

    private WriteLock writeLock = new ReentrantReadWriteLock().writeLock();
    private void initRefreshPool() {
        final ExecutorService oldPool = pool;
        writeLock.lock();
        try {
            final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
                    "EVCacheInMemoryCache-%d").build();
            pool = Executors.newFixedThreadPool(_poolSize.get(), factory);
            if(oldPool != null) oldPool.shutdown();
        } finally {
            writeLock.unlock();
        }
    }


    private void setupCache() {
        try {
            CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().recordStats();
            if(_cacheSize.get() > 0) {
                builder = builder.maximumSize(_cacheSize.get());
            }
            if(_exireAfterAccessDuration.get() > 0) {
                builder = builder.expireAfterAccess(_exireAfterAccessDuration.get(), TimeUnit.MILLISECONDS);
            } else if(_cacheDuration.get().intValue() > 0) {
                builder = builder.expireAfterWrite(_cacheDuration.get(), TimeUnit.MILLISECONDS);
            }  

            if(_refreshDuration.get() > 0) {
                builder = builder.refreshAfterWrite(_refreshDuration.get(), TimeUnit.MILLISECONDS);
            }
            initRefreshPool();
            final LoadingCache<String, T> newCache = builder.build(
                    new CacheLoader<String, T>() {
                        public T load(String key) throws  EVCacheException { 
                            try {
                                final T t = impl.doGet(key, tc);
                                if(t == null) throw new  DataNotFoundException("Data for key : " + key + " could not be loaded as it was not found in EVCache");
                                return t;
                            } catch (DataNotFoundException e) {
                                throw e;
                            } catch (EVCacheException e) {
                                log.error("EVCacheException while loading key -> "+ key, e);
                                throw e;
                            } catch (Exception e) {
                                log.error("EVCacheException while loading key -> "+ key, e);
                                throw new EVCacheException("key : " + key + " could not be loaded", e);
                            }
                        }

                        public ListenableFuture<T> reload(final String key, T prev) {
                            ListenableFutureTask<T> task = ListenableFutureTask.create(new Callable<T>() {
                                public T call() {
                                    try {
                                        final T t = load(key);
                                        if(t == null) {
                                            EVCacheMetricsFactory.getInstance().increment("EVCacheInMemoryCache" + "-" + appName + "-Reload-NotFound");
                                            return prev;
                                        } else {
                                            EVCacheMetricsFactory.getInstance().increment("EVCacheInMemoryCache" + "-" + appName + "-Reload-Success");
                                        }
                                        return t;
                                    } catch (EVCacheException e) {
                                        log.error("EVCacheException while reloading key -> "+ key, e);
                                        EVCacheMetricsFactory.getInstance().increment("EVCacheInMemoryCache" + "-" + appName + "-Reload-Fail");
                                        return prev;
                                    }
                                }
                            });
                            pool.execute(task);
                            return task;
                        }
                    });
            if(cache != null) newCache.putAll(cache.asMap());
            final Cache<String, T> currentCache = this.cache;
            this.cache = newCache;
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

        getCounter("hits").increment(stats.hitCount());
        getCounter("miss").increment(stats.missCount());
        getCounter("evictions").increment(stats.evictionCount());
        getCounter("requests").increment(stats.requestCount());

        getCounter("loadExceptionCount").increment(stats.loadExceptionCount());
        getCounter("loadCount").increment(stats.loadCount());
        getCounter("loadSuccessCount").increment(stats.loadSuccessCount());
        getCounter("totalLoadTime-ms").increment(cache.stats().totalLoadTime()/1000000);

        getGauge("hitrate").set(stats.hitRate());
        getGauge("loadExceptionRate").set(stats.loadExceptionRate());
        getGauge("averageLoadTime-ms").set(stats.averageLoadPenalty()/1000000);
        return size;
    }

    private void setupMonitoring(final String appName) {
        EVCacheMetricsFactory.getInstance().getRegistry().gauge(sizeId, this, EVCacheInMemoryCache::getSize);
    }

    private Counter getCounter(String name) {
        Counter counter = counterMap.get(name);
        if(counter != null) return counter;

        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.METRIC, name));
        counter = EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.IN_MEMORY, tags);
        counterMap.put(name, counter);
        return counter;
    }

    private Gauge getGauge(String name) {
        Gauge gauge = gaugeMap.get(name);
        if(gauge != null) return gauge;

        final List<Tag> tags = new ArrayList<Tag>(3);
        tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
        tags.add(new BasicTag(EVCacheMetricsFactory.METRIC, name));

        final Id id = EVCacheMetricsFactory.getInstance().getId(EVCacheMetricsFactory.IN_MEMORY, tags);
        gauge = EVCacheMetricsFactory.getInstance().getRegistry().gauge(id);
        gaugeMap.put(name, gauge);
        return gauge;
    }

    public T get(String key) throws ExecutionException {
        if (cache == null) return null;
        final T val = cache.get(key);
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

    public Map<String, T> getAll() {
        if (cache == null) return Collections.<String, T>emptyMap();
        return cache.asMap();
    }

    public static final class DataNotFoundException extends EVCacheException {
        private static final long serialVersionUID = 1800185311509130263L;

        public DataNotFoundException(String message) {
            super(message);
        }
    }
}
