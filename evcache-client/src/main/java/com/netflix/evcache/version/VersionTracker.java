package com.netflix.evcache.version;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.tag.BasicTagList;

@Singleton
public class VersionTracker implements Runnable {

	private static Logger log = LoggerFactory.getLogger(VersionTracker.class);
    private LongGauge versionGauge;
    private EVCacheClientPoolManager poolManager;
    
    @Inject
    public VersionTracker(EVCacheClientPoolManager poolManager) {
    	this.poolManager = poolManager;
    	poolManager.getEVCacheScheduledExecutor().schedule(this, 30, TimeUnit.SECONDS);
    }

    public void run() {
        // init the version information

    	if(versionGauge == null) {
    	    final String fullVersion;
    	    final String jarName;
            if(this.getClass().getPackage().getImplementationVersion() != null) {
                fullVersion = this.getClass().getPackage().getImplementationVersion();
            } else {
                fullVersion = "unknown";
            }

            if(this.getClass().getPackage().getImplementationVersion() != null) {
                jarName = this.getClass().getPackage().getImplementationTitle();
            } else {
                jarName = "unknown";
            }

            if(log.isErrorEnabled()) log.error("fullVersion : " + fullVersion + "; jarName : " + jarName);
	        versionGauge = EVCacheMetricsFactory.getLongGauge("evcache-client", BasicTagList.of("version", fullVersion, "jarName", jarName));
    	}
	    versionGauge.set(Long.valueOf(1));
    	poolManager.getEVCacheScheduledExecutor().schedule(this, 30, TimeUnit.SECONDS);
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj != null) && (obj.getClass() == getClass());
    }    
}
