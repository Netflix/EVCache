package com.netflix.evcache.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.netflix.spectator.api.Counter;
//import com.netflix.servo.DefaultMonitorRegistry;
//import com.netflix.servo.annotations.DataSourceType;
//import com.netflix.servo.monitor.BasicCounter;
//import com.netflix.servo.monitor.Counter;
//import com.netflix.servo.monitor.LongGauge;
//import com.netflix.servo.monitor.Monitor;
//import com.netflix.servo.monitor.MonitorConfig;
//import com.netflix.servo.monitor.Monitors;
//import com.netflix.servo.monitor.MonitorConfig.Builder;
//import com.netflix.servo.monitor.PercentileTimer;
//import com.netflix.servo.monitor.StepCounter;
//import com.netflix.servo.monitor.Timer;
//import com.netflix.servo.stats.StatsConfig;
//import com.netflix.servo.tag.BasicTagList;
//import com.netflix.servo.tag.Tag;
//import com.netflix.servo.tag.TagList;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
//import com.netflix.servo.tag.BasicTag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.PercentileTimer;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = { "NF_LOCAL_FAST_PROPERTY",
        "PMB_POSSIBLE_MEMORY_BLOAT" }, justification = "Creates only when needed")
public final class EVCacheMetricsFactory {

    private final Map<String, Number> monitorMap = new ConcurrentHashMap<String, Number>();
    private final Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
    private final Map<String, DistributionSummary> distributionSummaryMap = new ConcurrentHashMap<String, DistributionSummary>();
    private final Lock writeLock = (new ReentrantReadWriteLock()).writeLock();
    private final Map<String, Timer> timerMap = new HashMap<String, Timer>();

    private static final EVCacheMetricsFactory INSTANCE = new EVCacheMetricsFactory();

    private EVCacheMetricsFactory() {
        
    }
    
    public static EVCacheMetricsFactory getInstance() {
        return INSTANCE;
    }

    public Map<String, Number> getAllMonitor() {
        return monitorMap;
    }
    
    public Map<String, DistributionSummary> getAllDistributionSummaryMap() {
        return distributionSummaryMap;
    }

    public Registry getRegistry() {
        return Spectator.globalRegistry();
    }

    public AtomicLong getLongGauge(String name) {
        return getLongGauge(name, null);
    }

    public AtomicLong getLongGauge(String cName, Id id) {
        final String name = id != null ? cName + id.toString() : cName;
        AtomicLong gauge = (AtomicLong)monitorMap.get(name);
        if (gauge == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    gauge = (AtomicLong)monitorMap.get(name);
                } else {
                    if(id != null) {
                        gauge = getRegistry().gauge(name, id.tags(), new AtomicLong());
                    } else {
                        gauge = getRegistry().gauge(name, new AtomicLong());
                    }
                    monitorMap.put(name, gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }

    public Counter getCounter(String cName, Collection<Tag> tags) {
        final String name = tags != null ? cName + tags.toString() : cName;
        Counter counter = counterMap.get(name);
        if (counter == null) {
            writeLock.lock();
            try {
                if (counterMap.containsKey(name)) {
                    counter = counterMap.get(name);
                } else {
                    if(tags != null) {
                        counter = getRegistry().counter(name, tags);
                    } else {
                        counter = getRegistry().counter(name);
                    }
                    counterMap.put(name, counter);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return counter;
    }


    public Counter getCounter(String name) {
        return getCounter(name, null);
    }

    public void increment(String name) {
        final Counter counter = getCounter(name);
        counter.increment();
    }

    public void increment(String cName, Collection<Tag> tags) {
        final Counter counter = getCounter(cName, tags);
        counter.increment();
    }

    public Timer getPercentileTimer(String metric, Collection<Tag> tags) {
        final String name = tags != null ? metric + tags.toString() : metric;
        final Timer duration = timerMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (monitorMap.containsKey(name))
                return timerMap.get(name);
            else {
                final Id id = getRegistry().createId(metric).withTags(tags);
                final Timer _duration = PercentileTimer.get(getRegistry(), id);
                timerMap.put(name, _duration);
                return _duration;
            }
        } finally {
            writeLock.unlock();
        }
    }

//    public PercentileTimer getPercentileTimer(String appName, ServerGroup serverGroup, String metric) {
//        final String serverGroupName = (serverGroup != null ? serverGroup.getName() : "");
//        final String metricName = getMetricName(appName, null, metric);
//        final String name = metricName + serverGroupName + "type=PercentileTimer";
//        final PercentileTimer duration = (PercentileTimer) monitorMap.get(name);
//        if (duration != null) return duration;
//
//        writeLock.lock();
//        try {
//            if (monitorMap.containsKey(name))
//                return (PercentileTimer) monitorMap.get(name);
//            else {
//                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
//                        .withPublishMax(true).withPublishMin(true)
//                        .withPublishMean(true).withPublishCount(true).withSampleSize(sampleSize.get()).build();
//                final PercentileTimer _duration = new PercentileTimer(getMonitorConfig(metricName, appName, null, serverGroupName,
//                        metric), statsConfig, TimeUnit.MILLISECONDS);
//                monitorMap.put(name, _duration);
//                DefaultMonitorRegistry.getInstance().register(_duration);
//                return _duration;
//            }
//        } finally {
//            writeLock.unlock();
//        }
//    }
//
//    public String getMetricName(String appName, String cacheName, String metric) {
//        return appName + (cacheName == null ? "-" : "-" + cacheName + "-") + metric;
//    }
//
//    public MonitorConfig getMonitorConfig(String appName, String cacheName, String metric) {
//        return getMonitorConfig(getMetricName(appName, cacheName, metric), appName, cacheName, metric);
//    }
//
//    public MonitorConfig getMonitorConfig(String name, String appName, String cacheName, String metric) {
//        Builder builder = MonitorConfig.builder(name).withTag("APP", appName).withTag("METRIC", metric);
//        if (cacheName != null && cacheName.length() > 0) {
//            builder = builder.withTag("CACHE", cacheName);
//        }
//        return builder.build();
//    }
//
//    public MonitorConfig getMonitorConfig(String name, String appName, String cacheName, String serverGroup, String metric) {
//        Builder builder = MonitorConfig.builder(name).withTag("APP", appName).withTag("METRIC", metric);
//        if (cacheName != null && cacheName.length() > 0) {
//            builder = builder.withTag("CACHE", cacheName);
//        }
//        if (serverGroup != null && serverGroup.length() > 0) {
//            builder = builder.withTag("ServerGroup", serverGroup);
//        }
//        return builder.build();
//    }
//
//    public Timer getPercentileTimer(String name) {
//        Timer timer = timerMap.get(name);
//        if (timer != null) return timer;
//        writeLock.lock();
//        try {
//            if (timerMap.containsKey(name)) {
//                return timerMap.get(name);
//            } else {
//                final StatsConfig statsConfig = new StatsConfig.Builder().withPercentiles(new double[] { 95, 99 })
//                        .withPublishMax(true).withPublishMin(true).withPublishMean(true)
//                        .withPublishCount(true).withSampleSize(sampleSize.get()).build();
//                final MonitorConfig monitorConfig = MonitorConfig.builder(name).build();
//                timer = new PercentileTimer(monitorConfig, statsConfig, TimeUnit.MILLISECONDS);
//                DefaultMonitorRegistry.getInstance().register(timer);
//                timerMap.put(name, timer);
//                return timer;
//            }
//        } finally {
//            writeLock.unlock();
//        }
//    }
//
    public DistributionSummary getDistributionSummary(String name, Collection<Tag> tags) {
        final String metricName = (tags != null ) ? name + tags.toString() : name;
        final DistributionSummary _ds = distributionSummaryMap.get(metricName);
        if(_ds != null) return _ds;
        final Registry registry = Spectator.globalRegistry(); 
        if (registry != null) {
            final Id id = registry.createId(name).withTags(tags);
            final DistributionSummary ds = registry.distributionSummary(id);
            distributionSummaryMap.put(metricName, ds);
            return ds;
        }
        return null;
    }

}