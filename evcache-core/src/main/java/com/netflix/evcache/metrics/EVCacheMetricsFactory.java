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
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
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
    
    public Id getId(String name, Collection<Tag> tags) {
        List<Tag> tagList = new ArrayList<Tag>(1);
        tagList.addAll(tags);
        tagList.add(new BasicTag("owner", "evcache"));
        return getRegistry().createId(name, tagList);
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
                    List<Tag> tagList = new ArrayList<Tag>(1);
                    tagList.addAll(tags);
                    tagList.add(new BasicTag("owner", "evcache"));
                    counter = getRegistry().counter(cName, tagList);
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
            if (timerMap.containsKey(name))
                return timerMap.get(name);
            else {
                Id id = getId(metric, tags);
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
            Id id = getId(name, tags);
            final DistributionSummary ds = registry.distributionSummary(id);
            distributionSummaryMap.put(metricName, ds);
            return ds;
        }
        return null;
    }


    /**
     * External Metric Names
     */
    public static final String CALL                             = "evcache.client.call";
    public static final String FAST_FAIL                        = "evcache.client.fastfail";
    public static final String CONFIG                           = "evcache.client.config";
    public static final String DATA_SIZE                        = "evcache.client.datasize";
    public static final String IN_MEMORY                        = "evcache.client.inmemorycache";

    /**
     * Internal Metric Names
     */
    public static final String INTERNAL_CALL                    = "internal.evcache.client.call";
    public static final String INTERNAL_CONFIG                  = "internal.evcache.client.config";
    public static final String INTERNAL_PAUSE                   = "internal.evcache.client.pause";
    public static final String INTERNAL_LATCH                   = "internal.evcache.client.latch";
    public static final String INTERNAL                         = "internal.evcache.client";

    public static final String INTERNAL_NUM_CHUNK_SIZE          = "internal.evcache.client.chunking.numOfChunks";
    public static final String INTERNAL_CHUNK_DATA_SIZE         = "internal.evcache.client.chunking.dataSize";

    public static final String INTERNAL_POOL                    = "internal.evcache.client.pool";
    public static final String POOL_REFRESH                     = "refresh";
    public static final String POOL_SIZE                        = "size";
    public static final String POOL_ACTIVE                      = "activeConnections";
    public static final String POOL_INACTIVE                    = "inActiveConnections";
    public static final String POOL_IN_DISCOVERY                = "inDiscovery";
    public static final String POOL_IN_HASHING                  = "inHashing";
    public static final String POOL_READ_INSTANCES              = "readInstances";
    public static final String POOL_WRITE_INSTANCES             = "writeInstances";
    public static final String POOL_RECONCILE                   = "reconcile";
    public static final String POOL_CHANGED                     = "serverGroupChanged";
    public static final String POOL_SERVERGROUP_STATUS          = "serverGroupStatus";
    public static final String POOL_INIT_ERROR                  = "init.error";
    public static final String POOL_READ_Q_SIZE                 = "readQueue";
    public static final String POOL_WRITE_Q_SIZE                = "writeQueue";
    public static final String POOL_REFRESH_QUEUE_FULL          = "refreshOnQueueFull";
    public static final String POOL_REFRESH_ASYNC               = "refreshAsync";

    public static final String INTERNAL_ADD_CALL_FIXUP          = "internal.evcache.client.addCall.fixUp";

    /**
     * Metric Tags Names
     */
    public static final String CACHE                            = "evc.cache";
    public static final String SERVERGROUP                      = "evc.serverGroup";
    public static final String STATUS                           = "evc.status";

    public static final String CALL_TAG                         = "evc.call";
    public static final String LATCH                            = "evc.latch";
    public static final String CAUSE                            = "evc.cause";
    public static final String FAIL_COUNT                       = "evc.failCount";
    public static final String COMPLETE_COUNT                   = "evc.completeCount";
    public static final String RECONNECT_COUNT                  = "evc.reconnectCount";
    public static final String PAUSE_REASON                     = "evc.pauseReason";
    public static final String FETCH_AFTER_PAUSE                = "evc.fetchAfterPause";
    public static final String FAILED_SERVERGROUP               = "evc.failedServerGroup";
    public static final String CONFIG_NAME                      = "evc.config";
    public static final String HOST                             = "evc.host";
    public static final String CACHE_HIT                        = "evc.cacheHit";
    public static final String OPERATION                        = "evc.operation";
    public static final String OPERATION_TYPE                   = "evc.operationType";
    public static final String OPERATION_STATUS                 = "evc.operationStatus";
    public static final String NUMBER_OF_ATTEMPTS               = "evc.numberOfAttempts";
    public static final String ATTEMPT                          = "evc.attempt";
    public static final String NUMBER_OF_KEYS                   = "evc.numberOfKeys";
    public static final String METRIC                           = "evc.metric";
    public static final String PREFIX                           = "evc.prefix";

    /**
     * Metric Tags Values  
     */
    public static final String SIZE                             = "size";
    public static final String PORT                             = "port";
    public static final String CONNECT                          = "connect";
    public static final String DISCONNECT                       = "disconnect";
    public static final String SUCCESS                          = "success";
    public static final String TIMEOUT                          = "timeout";
    public static final String CANCELLED                        = "cancelled";
    public static final String THROTTLED                        = "throttled";
    public static final String ERROR                            = "error";
    public static final String READ                             = "read";
    public static final String WRITE                            = "write";
    public static final String YES                              = "yes";
    public static final String NO                               = "no";
    public static final String PARTIAL                          = "partial";
    public static final String UNKNOWN                          = "unknown";
    public static final String GC                               = "gc";
    public static final String NULL_CLIENT                      = "nullClient";
    public static final String NULL_ZONE                        = "nullZone";
    public static final String NULL_SERVERGROUP                 = "nullServerGroup";
    public static final String RECONNECT                        = "reconnect";
    public static final String CALLBACK                         = "callback";
    public static final String VERIFY                           = "verify";
    public static final String READ_QUEUE_FULL                  = "readQueueFull";
    public static final String INACTIVE_NODE                    = "inactiveNode";
    public static final String INCORRECT_CHUNKS                 = "incorrectNumOfChunks";
    public static final String INVALID_CHUNK_SIZE               = "invalidChunkSize";
    public static final String CHECK_SUM_ERROR                  = "checkSumError";
    public static final String NUM_CHUNK_SIZE                   = "numOfChunks";
    public static final String CHUNK_DATA_SIZE                  = "dataSize";
    public static final String NOT_AVAILABLE                    = "notAvailable";

    /**
     * Metric Tag Value for Operations
     */
    public static final String BULK_OPERATION                   = "BULK";
    public static final String GET_OPERATION                    = "GET";
    public static final String GET_AND_TOUCH_OPERATION          = "GET_AND_TOUCH";
    public static final String DELETE_OPERATION                 = "DELETE";
    public static final String TOUCH_OPERATION                  = "TOUCH";
    public static final String AOA_OPERATION                    = "APPEND_OR_ADD";
    public static final String SET_OPERATION                    = "SET";
    public static final String ADD_OPERATION                    = "ADD";
    public static final String REPLACE_OPERATION                = "REPLACE";

}
