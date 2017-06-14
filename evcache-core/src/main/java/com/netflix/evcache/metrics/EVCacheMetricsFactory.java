package com.netflix.evcache.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.netflix.spectator.api.BasicTag;
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

    public Map<String, Counter> getAllCounters() {
        return counterMap;
    }
    
    public Map<String, Timer> getAllTimers() {
        return timerMap;
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

    public AtomicLong getLongGauge(String cName, Collection<Tag> tags) {
        final String name = tags != null ? cName + tags.toString() : cName;
        AtomicLong gauge = (AtomicLong)monitorMap.get(name);
        if (gauge == null) {
            writeLock.lock();
            try {
                if (monitorMap.containsKey(name)) {
                    gauge = (AtomicLong)monitorMap.get(name);
                } else {
                    if(tags != null) {
                        gauge = getRegistry().gauge(cName, tags, new AtomicLong());
                    } else {
                        gauge = getRegistry().gauge(cName, new AtomicLong());
                    }
                    monitorMap.put(name, gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }
    
    public Id getId(String name, List<Tag> tags) {
        tags.add(new BasicTag("owner", "evcache"));
        return getRegistry().createId(name, tags);
    }

    public Counter getCounter(String cName, List<Tag> tags) {
        final String name = tags != null ? cName + tags.toString() : cName;
        Counter counter = counterMap.get(name);
        if (counter == null) {
            writeLock.lock();
            try {
                if (counterMap.containsKey(name)) {
                    counter = counterMap.get(name);
                } else {
                    if(tags == null) {
                        tags = new ArrayList<Tag>(1);
                    }
                    tags.add(new BasicTag("owner", "evcache"));
                    counter = getRegistry().counter(cName, tags);
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

    public void increment(String cName, List<Tag> tags) {
        final Counter counter = getCounter(cName, tags);
        counter.increment();
    }

    public Timer getPercentileTimer(String metric, Collection<Tag> tags) {
        final String name = tags != null ? metric + tags.toString() : metric;
        final Timer duration = timerMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (timerMap.containsKey(name))
                return timerMap.get(name);
            else {
                Id id = getRegistry().createId(metric);
                if(tags != null) id = id.withTags(tags); 
                final Timer _duration = PercentileTimer.get(getRegistry(), id);
                timerMap.put(name, _duration);
                return _duration;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public DistributionSummary getDistributionSummary(String name, Collection<Tag> tags) {
        final String metricName = (tags != null ) ? name + tags.toString() : name;
        final DistributionSummary _ds = distributionSummaryMap.get(metricName);
        if(_ds != null) return _ds;
        final Registry registry = Spectator.globalRegistry(); 
        if (registry != null) {
            Id id = registry.createId(name);
            if(tags != null) id = id.withTags(tags);
            final DistributionSummary ds = registry.distributionSummary(id);
            distributionSummaryMap.put(metricName, ds);
            return ds;
        }
        return null;
    }

}