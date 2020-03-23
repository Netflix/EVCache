package com.netflix.evcache.version;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class VersionTracker implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(VersionTracker.class);
    private AtomicLong versionGauge;
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

            if(log.isInfoEnabled()) log.info("fullVersion : " + fullVersion + "; jarName : " + jarName);
            final List<Tag> tagList = new ArrayList<Tag>(3);
            tagList.add(new BasicTag("version", fullVersion));
            tagList.add(new BasicTag("jarName", jarName));
	        versionGauge = EVCacheMetricsFactory.getInstance().getLongGauge("evcache-client", tagList);
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
