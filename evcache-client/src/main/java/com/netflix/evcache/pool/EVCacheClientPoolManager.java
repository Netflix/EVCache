package com.netflix.evcache.pool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.config.ConfigurationManager;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicStringProperty;
import com.netflix.evcache.connection.ConnectionFactoryProvider;
import com.netflix.evcache.connection.IConnectionFactoryProvider;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.util.EVCacheConfig;

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
    private static final Logger log = LoggerFactory.getLogger(EVCacheClientPoolManager.class);
    private static final DynamicIntProperty defaultReadTimeout = EVCacheConfig.getInstance().getDynamicIntProperty("default.read.timeout", 20);
    private final DynamicIntProperty defaultRefreshInterval = EVCacheConfig.getInstance().getDynamicIntProperty("EVCacheClientPoolManager.refresh.interval", 60);
    private static final DynamicStringProperty logEnabledApps = EVCacheConfig.getInstance().getDynamicStringProperty("EVCacheClientPoolManager.log.apps", "*");

    private volatile static EVCacheClientPoolManager instance;

    private final Map<String, EVCacheClientPool> poolMap = new ConcurrentHashMap<String, EVCacheClientPool>();
    private final Map<EVCacheClientPool, ScheduledFuture<?>> scheduledTaskMap = new HashMap<EVCacheClientPool, ScheduledFuture<?>>();
    private final ScheduledThreadPoolExecutor _scheduler;
    private final List<EVCacheEventListener> evcacheEventListenerList;
    private final IConnectionFactoryProvider connectionFactoryProvider;
    private final EVCacheNodeList evcacheNodeList;

    @Inject
    public EVCacheClientPoolManager(IConnectionFactoryProvider connectionFactoryprovider, EVCacheNodeList evcacheNodeList) {
        instance = this;
        
        try {
            ConfigurationManager.loadCascadedPropertiesFromResources("evcache");
        } catch (IOException e) {
            log.info("Default evcache configuration not loaded", e);
        }

        this.connectionFactoryProvider = connectionFactoryprovider;
        this.evcacheNodeList = evcacheNodeList;
        this.evcacheEventListenerList = new ArrayList<EVCacheEventListener>();

        final int poolSize = EVCacheConfig.getInstance().getDynamicIntProperty("default.refresher.poolsize", 1).get();
        final ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("EVCacheClientPoolManager_refresher-%d").build();
        defaultRefreshInterval.addCallback(new Runnable() {
            public void run() {
                refreshScheduler();
            }
        });
        _scheduler = new ScheduledThreadPoolExecutor(poolSize, factory);
        initAtStartup();
    }

    public IConnectionFactoryProvider getConnectionFactoryProvider() {
        return connectionFactoryProvider;
    }

    public void addEVCacheEventListener(EVCacheEventListener listener) {
        this.evcacheEventListenerList.add(listener);
    }

    public void removeEVCacheEventListener(EVCacheEventListener listener) {
        this.evcacheEventListenerList.remove(listener);
    }

    public List<EVCacheEventListener> getEVCacheEventListeners() {
        return this.evcacheEventListenerList;
    }

    private void refreshScheduler() {
        for (Iterator<EVCacheClientPool> itr = scheduledTaskMap.keySet().iterator(); itr.hasNext();) {
            final EVCacheClientPool pool = itr.next();
            final ScheduledFuture<?> task = scheduledTaskMap.get(pool);
            itr.remove();
            task.cancel(true);
            scheduleRefresh(pool);
        }
    }

    public static EVCacheClientPoolManager getInstance() {
        if (instance == null) {
            new EVCacheClientPoolManager(new ConnectionFactoryProvider(), new SimpleNodeListProvider());
            if (!EVCacheConfig.getInstance().getDynamicBooleanProperty("evcache.use.simple.node.list.provider", false).get()) {
                if(log.isDebugEnabled()) log.debug("Please make sure EVCacheClientPoolManager is injected first. This is not the appropriate way to init EVCacheClientPoolManager."
                        + " If you are using simple node list provider please set evcache.use.simple.node.list.provider property to true.", new Exception());
            }
        }
        return instance;
    }

    public void initAtStartup() {
        final String appsToInit = EVCacheConfig.getInstance().getDynamicStringProperty("evcache.appsToInit", "").get();
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
        if (app == null || (app = app.trim()).length() == 0) throw new IllegalArgumentException(
                "param app name null or space");
        final String APP = getAppName(app);
        if (poolMap.containsKey(APP)) return;

        final EVCacheClientPool pool = new EVCacheClientPool(APP, evcacheNodeList, this);
        scheduleRefresh(pool);
        poolMap.put(APP, pool);
    }

    private void scheduleRefresh(EVCacheClientPool pool) {
        final ScheduledFuture<?> task = _scheduler.scheduleWithFixedDelay(pool, 30, defaultRefreshInterval.get(), TimeUnit.SECONDS);
        scheduledTaskMap.put(pool, task);
    }

    /**
     * Given the appName get the EVCacheClientPool. If the app is already
     * created then will return the existing instance. If not one will be
     * created and returned.
     * 
     * @param app
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

    public void shutdown() {
        _scheduler.shutdown();
        for (EVCacheClientPool pool : poolMap.values()) {
            pool.shutdown();
        }
    }

    public boolean shouldLog(String appName) {
        if ("*".equals(logEnabledApps.get())) return true;
        if (logEnabledApps.get().indexOf(appName) != -1) return true;
        return false;
    }

    public static DynamicIntProperty getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    private String getAppName(String _app) {
        _app = _app.toUpperCase();
        final String app = ConfigurationManager.getConfigInstance().getString("EVCacheClientPoolManager." + _app + ".alias", _app).toUpperCase();
        if (log.isDebugEnabled()) log.debug("Original App Name : " + _app + "; Alias App Name : " + app);
        return app;
    }

}
