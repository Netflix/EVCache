package com.netflix.evcache.event.throttle;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.util.EVCacheConfig;

/**
 * <p>
 * To enable throttling on operations the set the below property
 *      <code>EVCacheThrottler.throttle.operations=true</code>
 * </p>
 * <p>
 * To throttle all operations specified in {@link Call} then add the {@link Call} (separated by comma(,)) to the below property.<br>
 *  <code>&lt;EVCache appName&gt;.throttle.calls=&lt;comma separated list of calls&gt;</code><br>
 *  <br>
 *  EX: To throttle {@link Call.GET} and {@link Call.DELETE} operations for EVCACHE_CRS set the below property
 *  <code>EVCACHE_CRS.throttle.calls=GET,DELETE</code>
 * 
 * @author smadappa
 */

@Singleton
public class ThrottleListener implements EVCacheEventListener {

    private static final Logger log = LoggerFactory.getLogger(ThrottleListener.class);
    private final Map<String, DynamicStringSetProperty> _ignoreOperationsMap;
    private final DynamicBooleanProperty enableThrottleOperations;
    private final EVCacheClientPoolManager poolManager;

    @Inject 
    public ThrottleListener(EVCacheClientPoolManager poolManager) {
        this.poolManager = poolManager;
        this._ignoreOperationsMap = new ConcurrentHashMap<String, DynamicStringSetProperty>();
        enableThrottleOperations = EVCacheConfig.getInstance().getDynamicBooleanProperty("EVCacheThrottler.throttle.operations", false);
        enableThrottleOperations.addCallback(new Runnable() {
            @Override
            public void run() {
                setupListener();
            }
        });
        if(enableThrottleOperations.get()) setupListener();
    }

    private void setupListener() {
        if(enableThrottleOperations.get()) {
            poolManager.addEVCacheEventListener(this);
        } else {
            poolManager.removeEVCacheEventListener(this);
        }
    }

    public void onStart(final EVCacheEvent e) {
    }

    @Override
    public boolean onThrottle(final EVCacheEvent e) {
        if(!enableThrottleOperations.get()) return false;

        final String appName = e.getAppName();
        DynamicStringSetProperty throttleCalls = _ignoreOperationsMap.get(appName);
        if(throttleCalls == null) {
            throttleCalls = new DynamicStringSetProperty(appName + ".throttle.calls", "");
            _ignoreOperationsMap.put(appName, throttleCalls );
        }
        if(throttleCalls.get().size() > 0 && throttleCalls.get().contains(e.getCall().name())) {
            if(log.isDebugEnabled()) log.debug("Call : " + e.getCall() + " is throttled");
            return true;
        }
        return false;
    }

    public void onComplete(EVCacheEvent e) {
    }

    public void onError(EVCacheEvent e, Throwable t) {
    }

}