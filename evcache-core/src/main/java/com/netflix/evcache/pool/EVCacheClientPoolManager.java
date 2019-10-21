package com.netflix.evcache.pool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.EVCacheImpl;
import com.netflix.evcache.EVCacheInMemoryCache;
import com.netflix.evcache.connection.ConnectionFactoryBuilder;
import com.netflix.evcache.connection.IConnectionBuilder;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.util.EVCacheConfig;

import net.spy.memcached.transcoders.Transcoder;

/**
 * A manager that holds Pools for each EVCache app. When this class is
 * initialized all the EVCache apps defined in the property evcache.appsToInit
 * will be initialized and added to the pool. If a service knows all the EVCache
 * app it uses, then it can define this property and pass a list of EVCache apps
 * that needs to be initialized.
 *
 * An EVCache app can also be initialized by Injecting
 * <code>EVCacheClientPoolManager</code> and calling <code>
 *      initEVCache(<app name>)
 *      </code>
 *
 * This typically should be done in the client libraries that need to initialize
 * an EVCache app. For Example VHSViewingHistoryLibrary in its initLibrary
 * initializes EVCACHE_VH by calling
 *
 * <pre>
 *      {@literal @}Inject
 *      public VHSViewingHistoryLibrary(EVCacheClientPoolManager instance,...) {
 *          ....
 *          instance.initEVCache("EVCACHE_VH");
 *          ...
 *      }
 * </pre>
 *
 * @author smadappa
 *
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "DM_CONVERT_CASE", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD" })
@Singleton
public class EVCacheClientPoolManager {
    /**
     * <b>NOTE : Should be the only static referenced variables</b> 
     * **/ 
    private static final Logger log = LoggerFactory.getLogger(EVCacheClientPoolManager.class);
    private volatile static EVCacheClientPoolManager instance;

    private final Property<Integer> defaultReadTimeout;
    private final Property<String> logEnabledApps;
    private final Property<Integer> defaultRefreshInterval;
    private final Map<String, EVCacheClientPool> poolMap = new ConcurrentHashMap<String, EVCacheClientPool>();
    private final Map<EVCacheClientPool, ScheduledFuture<?>> scheduledTaskMap = new HashMap<EVCacheClientPool, ScheduledFuture<?>>();
    private final EVCacheScheduledExecutor asyncExecutor;
    private final EVCacheExecutor syncExecutor;
    private final List<EVCacheEventListener> evcacheEventListenerList;
    private final IConnectionBuilder connectionFactoryProvider;
    private final EVCacheNodeList evcacheNodeList;
    private final EVCacheConfig evcConfig;

    @Inject
    public EVCacheClientPoolManager(IConnectionBuilder connectionFactoryprovider, EVCacheNodeList evcacheNodeList, EVCacheConfig evcConfig) {
        instance = this;

        this.connectionFactoryProvider = connectionFactoryprovider;
        this.evcacheNodeList = evcacheNodeList;
        this.evcConfig = evcConfig;
        this.evcacheEventListenerList = new CopyOnWriteArrayList<EVCacheEventListener>();
        
        this.defaultReadTimeout = EVCacheConfig.getInstance().getPropertyRepository().get("default.read.timeout", Integer.class).orElse(20);
        this.logEnabledApps = EVCacheConfig.getInstance().getPropertyRepository().get("EVCacheClientPoolManager.log.apps", String.class).orElse("*");
        this.defaultRefreshInterval = EVCacheConfig.getInstance().getPropertyRepository().get("EVCacheClientPoolManager.refresh.interval", Integer.class).orElse(60);

        this.asyncExecutor = new EVCacheScheduledExecutor(Runtime.getRuntime().availableProcessors(),Runtime.getRuntime().availableProcessors(), 30, TimeUnit.SECONDS, new ThreadPoolExecutor.CallerRunsPolicy(), "scheduled");
        asyncExecutor.prestartAllCoreThreads();
        this.syncExecutor = new EVCacheExecutor(Runtime.getRuntime().availableProcessors(),Runtime.getRuntime().availableProcessors(), 30, TimeUnit.SECONDS, new ThreadPoolExecutor.CallerRunsPolicy(), "pool");
        syncExecutor.prestartAllCoreThreads();

        initAtStartup();
    }

    public IConnectionBuilder getConnectionFactoryProvider() {
        return connectionFactoryProvider;
    }

    public void addEVCacheEventListener(EVCacheEventListener listener) {
        this.evcacheEventListenerList.add(listener);
    }

    public void addEVCacheEventListener(EVCacheEventListener listener, int index) {
        if(index < evcacheEventListenerList.size()) {
            this.evcacheEventListenerList.add(index, listener);
        } else {
            this.evcacheEventListenerList.add(listener);
        }
    }

    public void removeEVCacheEventListener(EVCacheEventListener listener) {
        this.evcacheEventListenerList.remove(listener);
    }

    public List<EVCacheEventListener> getEVCacheEventListeners() {
        return this.evcacheEventListenerList;
    }
    
    public EVCacheConfig getEVCacheConfig() {
        return this.evcConfig;
    }

    /**
     * @deprecated. Please use DependencyInjection (@Inject) to obtain
     * {@link EVCacheClientPoolManager}. The use of this can result in
     * unintended behavior where you will not be able to talk to evcache
     * instances.
     */
    @Deprecated
    public static EVCacheClientPoolManager getInstance() {
        if (instance == null) {
            new EVCacheClientPoolManager(new ConnectionFactoryBuilder(), new SimpleNodeListProvider(), EVCacheConfig.getInstance());
            if (!EVCacheConfig.getInstance().getPropertyRepository().get("evcache.use.simple.node.list.provider", Boolean.class).orElse(false).get()) {
                if(log.isDebugEnabled()) log.debug("Please make sure EVCacheClientPoolManager is injected first. This is not the appropriate way to init EVCacheClientPoolManager."
                        + " If you are using simple node list provider please set evcache.use.simple.node.list.provider property to true.", new Exception());
            }
        }
        return instance;
    }

    public void initAtStartup() {
        final String appsToInit = EVCacheConfig.getInstance().getPropertyRepository().get("evcache.appsToInit", String.class).orElse("").get();
        if (appsToInit != null && appsToInit.length() > 0) {
            final StringTokenizer apps = new StringTokenizer(appsToInit, ",");
            while (apps.hasMoreTokens()) {
                final String app = getAppName(apps.nextToken());
                if (log.isDebugEnabled()) log.debug("Initializing EVCache - " + app);
                initEVCache(app);
            }
        }
    }

    /**
     * Will init the given EVCache app call. If one is already initialized for
     * the given app method returns without doing anything.
     *
     * @param app
     *            - name of the evcache app
     */
    public final synchronized void initEVCache(String app) {
        if (app == null || (app = app.trim()).length() == 0) throw new IllegalArgumentException("param app name null or space");
        final String APP = getAppName(app);
        if (poolMap.containsKey(APP)) return;
        final EVCacheClientPool pool = new EVCacheClientPool(APP, evcacheNodeList, asyncExecutor, this);
        scheduleRefresh(pool);
        poolMap.put(APP, pool);
    }

    private void scheduleRefresh(EVCacheClientPool pool) {
        final ScheduledFuture<?> task = asyncExecutor.scheduleWithFixedDelay(pool, 30, defaultRefreshInterval.get(), TimeUnit.SECONDS);
        scheduledTaskMap.put(pool, task);
    }

    /**
     * Given the appName get the EVCacheClientPool. If the app is already
     * created then will return the existing instance. If not one will be
     * created and returned.
     *
     * @param _app
     *            - name of the evcache app
     * @return the Pool for the give app.
     * @throws IOException
     */
    public EVCacheClientPool getEVCacheClientPool(String _app) {
        final String app = getAppName(_app);
        final EVCacheClientPool evcacheClientPool = poolMap.get(app);
        if (evcacheClientPool != null) return evcacheClientPool;
        initEVCache(app);
        return poolMap.get(app);
    }

    public Map<String, EVCacheClientPool> getAllEVCacheClientPool() {
        return new HashMap<String, EVCacheClientPool>(poolMap);
    }

    @PreDestroy
    public void shutdown() {
        asyncExecutor.shutdown();
        syncExecutor.shutdown();
        for (EVCacheClientPool pool : poolMap.values()) {
            pool.shutdown();
        }
    }

    public boolean shouldLog(String appName) {
        if ("*".equals(logEnabledApps.get())) return true;
        if (logEnabledApps.get().indexOf(appName) != -1) return true;
        return false;
    }

    public Property<Integer> getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    public Property<Integer> getDefaultRefreshInterval() {
        return defaultRefreshInterval;
    }

    public EVCacheScheduledExecutor getEVCacheScheduledExecutor() {
        return asyncExecutor;
    }

    public EVCacheExecutor getEVCacheExecutor() {
        return syncExecutor;
    }

    private String getAppName(String _app) {
        _app = _app.toUpperCase();
        final String app = EVCacheConfig.getInstance().getPropertyRepository().get("EVCacheClientPoolManager." + _app + ".alias", String.class).orElse(_app).get().toUpperCase();
        if (log.isDebugEnabled()) log.debug("Original App Name : " + _app + "; Alias App Name : " + app);
        if(app != null && app.length() > 0) return app.toUpperCase();
        return _app;
    }

    private WriteLock writeLock = new ReentrantReadWriteLock().writeLock();
    private final Map<String, EVCacheInMemoryCache<?>> inMemoryMap = new ConcurrentHashMap<String, EVCacheInMemoryCache<?>>();
    @SuppressWarnings("unchecked")
    public <T> EVCacheInMemoryCache<T> createInMemoryCache(String appName, Transcoder<T> tc, EVCacheImpl impl) {
        EVCacheInMemoryCache<T> cache = (EVCacheInMemoryCache<T>) inMemoryMap.get(appName);
        if(cache == null) {
            writeLock.lock();
            if((cache = getInMemoryCache(appName)) == null) {
                cache = new EVCacheInMemoryCache<T>(appName, tc, impl);
                inMemoryMap.put(appName, cache);
            }
            writeLock.unlock();
        }
        return cache;
    }

    @SuppressWarnings("unchecked")
    public <T> EVCacheInMemoryCache<T> getInMemoryCache(String appName) {
        return (EVCacheInMemoryCache<T>) inMemoryMap.get(appName);
    }

}
