package com.netflix.evcache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
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
    private final ChainedDynamicProperty.IntProperty _cacheDuration; // The key will be cached for this long
    private final DynamicIntProperty _refreshDuration, _exireAfterAccessDuration;
    private final DynamicIntProperty _cacheSize; // This many items will be cached
    private final DynamicIntProperty _poolSize; // This many threads will be initialized to fetch data from evcache async
    private final String appName;

    private LoadingCache<String, T> cache;
    private ExecutorService pool = null;
    
    private final Transcoder<T> tc;
    private final EVCacheImpl impl;

    public EVCacheInMemoryCache(String appName, Transcoder<T> tc, EVCacheImpl impl) {
        this.appName = appName;
        this.tc = tc;
        this.impl = impl;

        final Runnable setupCache = new Runnable() {
            public void run() {
                setupCache();
            }
        };

        this._cacheDuration = EVCacheConfig.getInstance().getChainedIntProperty(appName + ".inmemory.cache.duration.ms", appName + ".inmemory.expire.after.write.duration.ms", 0, setupCache);

        this._exireAfterAccessDuration = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.expire.after.access.duration.ms", 0);
        this._exireAfterAccessDuration.addCallback(setupCache);

        this._refreshDuration = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.refresh.after.write.duration.ms", 0);
        this._refreshDuration.addCallback(setupCache);

        this._cacheSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".inmemory.cache.size", 100);
        this._cacheSize.addCallback(setupCache);

        this._poolSize = EVCacheConfig.getInstance().getDynamicIntProperty(appName + ".thread.pool.size", 5);
        this._poolSize.addCallback(new Runnable() {
            public void run() {
            	initRefreshPool();
            }
        });

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

    private void register(Monitor<?> monitor) {
        final MonitorRegistry registry = DefaultMonitorRegistry.getInstance();
        if (registry.isRegistered(monitor)) registry.unregister(monitor);
        // This will ensure old cache values are unregistered
        registry.register(monitor);
    }

    private MonitorConfig getMonitorConfig(String appName, String metric, Tag tag) {
        final Builder builder = MonitorConfig.builder("EVCacheInMemoryCache" + "-" + appName + "-" + metric).withTag(tag).withTag(EVCacheMetricsFactory.OWNER);
        return builder.build();
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
                        public T load(String key) throws EVCacheException { 
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
  	                        	    T t = load(key);
  	                        	    if(t == null) {
  	                        	    	EVCacheMetricsFactory.increment(appName, null, null, "EVCacheInMemoryCache" + "-" + appName + "-Reload-NotFound");
  	                        	    	return prev;
  	                        	    } else {
  	                        	    	EVCacheMetricsFactory.increment(appName, null, null, "EVCacheInMemoryCache" + "-" + appName + "-Reload-Success");
  	                        	    }
  									return t;
  								} catch (EVCacheException e) {
  									log.error("EVCacheException while reloading key -> "+ key, e);
  									EVCacheMetricsFactory.increment(appName, null, null, "EVCacheInMemoryCache" + "-" + appName + "-Reload-Fail");
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

    private LoadingCache<String, T> getCache() {
    	return cache;
    }
    
    private CacheStats getStats() {
    	return cache.stats();
    }

    private void setupMonitoring(final String appName) {
        final StepCounter sizeCounter = new StepCounter(getMonitorConfig(appName, "size", DataSourceType.GAUGE)) {
            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                log.debug("Current size is : " + getCache().size());
                return Long.valueOf(getCache().size());
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
                if (getCache() == null) return Long.valueOf(0);
                return Long.valueOf(getStats().requestCount());
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
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().hitRate());
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
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().hitCount());
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
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().missCount());
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
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().evictionCount());
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
                config = getMonitorConfig(appName, "loadExceptionCount", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().loadExceptionCount());
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
                config = getMonitorConfig(appName, "loadCount", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().loadCount());
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
                config = getMonitorConfig(appName, "loadSuccessCount", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().loadSuccessCount());
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
                config = getMonitorConfig(appName, "totalLoadTime-ms", DataSourceType.COUNTER);
            }

            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().totalLoadTime()/1000000);
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

        final StepCounter loadExceptionRate = new StepCounter(getMonitorConfig(appName, "loadExceptionRate", DataSourceType.GAUGE)) {
            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().loadExceptionRate());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        register(loadExceptionRate);

        final StepCounter averageLoadTime = new StepCounter(getMonitorConfig(appName, "averageLoadTime-ms", DataSourceType.GAUGE)) {
            @Override
            public Number getValue() {
                if (getCache() == null) return Long.valueOf(0);
                return Double.valueOf(getStats().averageLoadPenalty()/1000000);
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        register(averageLoadTime);
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
