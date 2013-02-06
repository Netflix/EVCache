package com.netflix.evcache.pool;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ConfigurationManager;

/**
 * <p>A manager that holds pool of {@link EVCacheClient} instances for each EVCache app. When this class is initialized all the {@link EVCache} apps defined in the property 
 * evcache.appsToInit will be initialized and added to the pool.   
 * 
 *  <p>An {@link EVCache} app can also be initialized by calling
 *  <p>{@code EVCacheClientPoolManager.getInstance().initEVCache(<app name>);}
 *      
 *  <p>This typically should be done in the client libraries that need to initialize an EVCache app. 
 *  For Example ViewingHistoryLibrary in its initLibrary initializes EVCACHE_VIEW_HIST by calling
 *  
 *      <p>{@code EVCacheClientPoolManager.getInstance().initEVCache("EVCACHE_VIEW_HIST");}
 */

public class EVCacheClientPoolManager {
    private static final Logger log = LoggerFactory.getLogger(EVCacheClientPoolManager.class);
    private static final EVCacheClientPoolManager instance = new EVCacheClientPoolManager();
    private final Map<String, EVCacheClientPool> poolMap = new ConcurrentHashMap<String, EVCacheClientPool>();
    private final ReentrantLock lock = new ReentrantLock();
    private final String evcachePoolProvider;
    
    private EVCacheClientPoolManager() {
        try {
        	evcachePoolProvider = ConfigurationManager.getConfigInstance().getString("evcache.pool.provider", "com.netflix.evcache.pool.eureka.EVCacheClientPoolImpl");
            init();
        } catch (ConfigurationException e) {
            log.error("Could not load the config file. Will not be able to init EVCaches!!!!", e);
            throw new IllegalStateException("Could not load the config file. Will not be able to init EVCaches!!!!", e);
        } catch (IOException e) {
            log.error("Unable to init EVCaches!!!!", e);
            throw new IllegalStateException("Could not find the config file. Will not be able to init EVCaches!!!!", e);
        }
    }

    public static EVCacheClientPoolManager getInstance() {
        return instance;
    }
    
    private void init() throws ConfigurationException, IOException {
        final String appsToInit = ConfigurationManager.getConfigInstance().getString("evcache.appsToInit");
        if(appsToInit == null)  return;
        final StringTokenizer apps = new StringTokenizer(appsToInit, ",");
        while(apps.hasMoreTokens()) {
            final String app = apps.nextToken().toUpperCase();
            if(log.isInfoEnabled()) log.info("Initializing EVCache - " + app);
            initEVCache(app);
        }
    }
    
    /**
     * Will init the given EVCache app call. If one is already initialized for the given app method returns without doing anything.  
     *  
     * @param app - name of the evcache app
     */
    public void initEVCache(String appName) {
        if(poolMap.containsKey(appName)) return;
        lock.lock();
        try {
            if(poolMap.containsKey(appName)) return;
            final EVCacheClientPool pool = (EVCacheClientPool)(Class.forName(evcachePoolProvider).newInstance());
            pool.init(appName);
            poolMap.put(appName, pool);
        } catch (Exception ex) {
        	log.error("Exception initialzing " + evcachePoolProvider + " for app " + appName, ex);
		} finally {
            lock.unlock();
        }
    }
    
    /**
     * Given the appName get the EVCacheClientPool. If the app is already created then will return the existing instance. If not one will be created and returned.  
     *  
     * @param app - name of the evcache app
     * @return the Pool for the give app.
     * @throws IOException
     */
    public EVCacheClientPool getEVCacheClientPool(String app) {
        final EVCacheClientPool evcacheClientPool = poolMap.get(app);
        if(evcacheClientPool != null) return evcacheClientPool;
        initEVCache(app);
        return poolMap.get(app);
    }

    public Map<String, EVCacheClientPool> getAllEVCacheClientPool() {
        return Collections.unmodifiableMap(poolMap);
    }

    public void shutdown() {
        for(EVCacheClientPool pool : poolMap.values()) {
            pool.shutdown();
        }
    }
}
