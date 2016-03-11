package com.netflix.evcache.metrics;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCache.Call;
import com.netflix.servo.monitor.StatsTimer;
import com.netflix.servo.monitor.StepCounter;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings("REC_CATCH_EXCEPTION")
public class EVCacheMetrics implements EVCacheMetricsMBean, Stats {
    private static final Logger log = LoggerFactory.getLogger(EVCacheMetrics.class);

    private final String appName, cacheName;
    private StepCounter getCallsCounter, bulkCallsCounter, bulkHitsCounter, getHitsCounter, setCallsCounter, replaceCallCounter, delCallsCounter;
    private StepCounter bulkMissCounter, getMissCounter;
    private StatsTimer getDuration, bulkDuration;

    EVCacheMetrics(final String appName, String _cacheName) {
        this.appName = appName;
        this.cacheName = (_cacheName == null) ? "" : _cacheName;

        setupMonitoring(appName, cacheName);
    }

    public void operationCompleted(Call op, long duration) {
        if (op == Call.GET || op == Call.GET_AND_TOUCH) {
            getCallCounter().increment();
            getGetCallDuration().record(duration);
        } else if (op == Call.SET) {
            getSetCallCounter().increment();
        } else if (op == Call.REPLACE) {
            getReplaceCallCounter().increment();
        } else if (op == Call.DELETE) {
            getDeleteCallCounter().increment();
        } else if (op == Call.BULK) {
            getBulkCounter().increment();
            getBulkCallDuration().record(duration);
        }
    }

    private void setupMonitoring(String _appName, String _cacheName) {
        try {
            String mBeanName = "com.netflix.evcache:Group=" + _appName + ",SubGroup=AtlasStats";
            if (_cacheName != null) mBeanName = mBeanName + ",SubSubGroup=" + _cacheName;
            final ObjectName mBeanObj = ObjectName.getInstance(mBeanName);
            final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mbeanServer.isRegistered(mBeanObj)) {
                if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanObj + " has been registered. Will unregister the previous instance and register a new one.");
                mbeanServer.unregisterMBean(mBeanObj);
            }
            mbeanServer.registerMBean(this, mBeanObj);
            if (log.isDebugEnabled()) log.debug("MBEAN with name " + mBeanObj + " has been registered.");
        } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug(e.getMessage(), e);
        }
    }

    private StepCounter getCallCounter() {
        if (this.getCallsCounter != null) return this.getCallsCounter;

        this.getCallsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "GetCall");
        getHitCounter();

        return getCallsCounter;
    }

    private StepCounter getHitCounter() {
        if (this.getHitsCounter != null) return this.getHitsCounter;

        this.getHitsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "GetHit");
        return getHitsCounter;
    }

    private StepCounter getMissCounter() {
        if (this.getMissCounter != null) return this.getMissCounter;

        this.getMissCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "GetMiss");
        return getMissCounter;
    }

    private StepCounter getBulkCounter() {
        if (this.bulkCallsCounter != null) return this.bulkCallsCounter;

        this.bulkCallsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "BulkCall");
        return bulkCallsCounter;
    }

    private StepCounter getBulkHitCounter() {
        if(this.bulkHitsCounter != null) return this.bulkHitsCounter;

        this.bulkHitsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "BulkHit");
        return bulkHitsCounter;
    }

    private StepCounter getBulkMissCounter() {
        if(this.bulkMissCounter != null) return this.bulkMissCounter;

        this.bulkMissCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "BulkMiss");
        return bulkMissCounter;
    }

    private StepCounter getSetCallCounter() {
        if (this.setCallsCounter != null) return this.setCallsCounter;

        this.setCallsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "SetCall");
        return setCallsCounter;
    }

    private StepCounter getReplaceCallCounter() {
        if (this.replaceCallCounter != null) return this.replaceCallCounter;

        this.replaceCallCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "ReplaceCall");
        return replaceCallCounter;
    }

    private StepCounter getDeleteCallCounter() {
        if (this.delCallsCounter != null) return this.delCallsCounter;

        this.delCallsCounter = EVCacheMetricsFactory.getStepCounter(appName, cacheName, "DeleteCall");
        return delCallsCounter;
    }

    private StatsTimer getGetCallDuration() {
        if (getDuration != null) return getDuration;

        this.getDuration = EVCacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyGet");
        return getDuration;
    }

    private StatsTimer getBulkCallDuration() {
        if (bulkDuration != null) return bulkDuration;

        this.bulkDuration = EVCacheMetricsFactory.getStatsTimer(appName, cacheName, "LatencyBulk");
        return bulkDuration;
    }

    public long getGetCalls() {
        return getCallCounter().getValue().longValue();
    }

    public long getCacheHits() {
        return getHitCounter().getValue().longValue();
    }

    public long getCacheMiss() {
        return getMissCounter().getValue().longValue();
    }

    public long getBulkCalls() {
        return getBulkCounter().getValue().longValue();
    }

    public long getBulkHits() {
        return getBulkHitCounter().getValue().longValue();
    }

    public long getBulkMiss() {
        return getBulkMissCounter().getValue().longValue();
    }

    public long getSetCalls() {
        return getSetCallCounter().getValue().longValue();
    }

    public void cacheHit(Call call) {
        if (call == Call.BULK) {
            this.getBulkHitCounter().increment();
        } else {
            this.getHitCounter().increment();
        }
    }

    public void cacheMiss(Call call) {
        if (call == Call.BULK) {
            this.getBulkMissCounter().increment();
        } else {
            this.getMissCounter().increment();
        }
    }

    public long getGetDuration() {
        return getGetCallDuration().getValue().longValue();
    }

    public long getBulkDuration() {
        return getBulkCallDuration().getValue().longValue();
    }

    public String toString() {
        return "EVCacheMetrics [ AppName=" + appName + ",  CachePrefix=" + cacheName + ", getCalls=" + getCallCounter() + ", bulkCalls="
                + getBulkCounter() + ", setCalls=" + getSetCallCounter() + ", cacheHits=" + getHitCounter() + ", cacheMiss=" + getMissCounter() 
                + ", bulkHits=" + getBulkHitCounter() + ", bulkMiss=" + getBulkMissCounter() + ", deleteCalls=" + getDeleteCallCounter()
                + ", getDuration=" + getGetCallDuration() + ", bulkDuration=" + getBulkCallDuration() + ", replaceCalls=" + getReplaceCallCounter() + "]";
    }

    public double getHitRate() {
        return (getCacheHits() / getGetCalls()) * 100;
    }

    public double getBulkHitRate() {
        return (getBulkHits() / getBulkCalls()) * 100;
    }
}