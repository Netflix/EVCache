package com.netflix.evcache.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.PercentileTimer;
import com.netflix.spectator.ipc.IpcStatus;

import net.spy.memcached.ops.StatusCode;

@SuppressWarnings("deprecation")
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
                        final Id id = getId(cName, tags);
                        gauge = getRegistry().gauge(id, new AtomicLong());
                    } else {
                        final Id id = getId(cName, null);
                        gauge = getRegistry().gauge(id, new AtomicLong());
                    }
                    monitorMap.put(name, gauge);
                }
            } finally {
                writeLock.unlock();
            }
        }
        return gauge;
    }
    
    private void addCommonTags(List<Tag> tagList) {
        tagList.add(new BasicTag(OWNER, "evcache"));
        final String additionalTags = EVCacheConfig.getInstance().getPropertyRepository().get("evcache.additional.tags", String.class).orElse(null).get();
        if(additionalTags != null && additionalTags.length() > 0) {
            final StringTokenizer st = new StringTokenizer(additionalTags, ","); 
            while(st.hasMoreTokens()) {
                final String token = st.nextToken().trim();
                String val = System.getProperty(token);
                if(val == null) val = System.getenv(token);
                if(val != null) tagList.add(new BasicTag(token, val));
            }
        }        
    }

    public void addAppNameTags(List<Tag> tagList, String appName) {
        tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
        tagList.add(new BasicTag(EVCacheMetricsFactory.ID, appName));
    }

    public Id getId(String name, Collection<Tag> tags) {
        final List<Tag> tagList = new ArrayList<Tag>();
        if(tags != null) tagList.addAll(tags);
        addCommonTags(tagList);
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
                    List<Tag> tagList = new ArrayList<Tag>(tags.size() + 1);
                    tagList.addAll(tags);
                    final Id id = getId(cName, tagList);
                    counter = getRegistry().counter(id);
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

    @Deprecated
    public Timer getPercentileTimer(String metric, Collection<Tag> tags) {
        return getPercentileTimer(metric, tags, Duration.ofMillis(100));
    }

    public Timer getPercentileTimer(String metric, Collection<Tag> tags, Duration max) {
        final String name = tags != null ? metric + tags.toString() : metric;
        final Timer duration = timerMap.get(name);
        if (duration != null) return duration;

        writeLock.lock();
        try {
            if (timerMap.containsKey(name))
                return timerMap.get(name);
            else {
                Id id = getId(metric, tags);
                final Timer _duration = PercentileTimer.builder(getRegistry()).withId(id).withRange(Duration.ofNanos(100000), max).build();
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

    public String getStatusCode(StatusCode sc) {
        switch(sc) { 
            case CANCELLED : 
                return IpcStatus.cancelled.name();

            case TIMEDOUT : 
                return IpcStatus.timeout.name();

            case INTERRUPTED : 
                return EVCacheMetricsFactory.INTERRUPTED;

            case SUCCESS : 
                return IpcStatus.success.name();

            case ERR_NOT_FOUND:
                return "not_found";
            case ERR_EXISTS:
                return "exists";
            case ERR_2BIG:
                return "too_big";
            case ERR_INVAL:
                return "invalid";
            case ERR_NOT_STORED:
                return "not_stored";
            case ERR_DELTA_BADVAL:
                return "bad_value";
            case ERR_NOT_MY_VBUCKET:
                return "not_my_vbucket";
            case ERR_UNKNOWN_COMMAND:
                return "unknown_command";
            case ERR_NO_MEM:
                return "no_mem";
            case ERR_NOT_SUPPORTED:
                return "not_supported";
            case ERR_INTERNAL:
                return "error_internal";
            case ERR_BUSY:
                return "error_busy";
            case ERR_TEMP_FAIL:
                return "temp_failure";
            case ERR_CLIENT :
                return "error_client";
            default : 
                return sc.name().toLowerCase();
        }
    }

    
    /**
     * External Metric Names
     */
    public static final String OVERALL_CALL                         = "evcache.client.call";
    public static final String OVERALL_KEYS_SIZE                    = "evcache.client.call.keys.size";

    /**
     * External IPC Metric Names
     */
    public static final String IPC_CALL                             = "ipc.client.call";
    public static final String IPC_SIZE_INBOUND                     = "ipc.client.call.size.inbound";
    public static final String IPC_SIZE_OUTBOUND                    = "ipc.client.call.size.outbound";

    public static final String OWNER                                = "owner";
    public static final String ID                                   = "id";

    /**
     * Internal Metric Names
     */
    public static final String CONFIG                               = "internal.evc.client.config";
    public static final String DATA_SIZE                            = "internal.evc.client.datasize";
    public static final String IN_MEMORY                            = "internal.evc.client.inmemorycache";
    public static final String FAST_FAIL                            = "internal.evc.client.fastfail";
    public static final String INTERNAL_OPERATION                   = "internal.evc.client.operation";
    public static final String INTERNAL_PAUSE                       = "internal.evc.client.pause";
    public static final String INTERNAL_LATCH                       = "internal.evc.client.latch";
    public static final String INTERNAL_LATCH_VERIFY                = "internal.evc.client.latch.verify";
    public static final String INTERNAL_FAIL                        = "internal.evc.client.fail";
    public static final String INTERNAL_EVENT_FAIL                  = "internal.evc.client.event.fail";
    public static final String INTERNAL_RECONNECT                   = "internal.evc.client.reconnect";
    public static final String INTERNAL_EXECUTOR                    = "internal.evc.client.executor";
    public static final String INTERNAL_EXECUTOR_SCHEDULED          = "internal.evc.client.scheduledExecutor";
    public static final String INTERNAL_POOL_INIT_ERROR             = "internal.evc.client.init.error";

    public static final String INTERNAL_NUM_CHUNK_SIZE              = "internal.evc.client.chunking.numOfChunks";
    public static final String INTERNAL_CHUNK_DATA_SIZE             = "internal.evc.client.chunking.dataSize";
    public static final String INTERNAL_ADD_CALL_FIXUP              = "internal.evc.client.addCall.fixUp";

    public static final String INTERNAL_POOL_SG_CONFIG              = "internal.evc.client.pool.asg.config";
    public static final String INTERNAL_POOL_CONFIG                 = "internal.evc.client.pool.config";
    public static final String INTERNAL_POOL_REFRESH                = "internal.evc.client.pool.refresh";

    public static final String INTERNAL_STATS                       = "internal.evc.client.stats";
    
    public static final String INTERNAL_TTL                         = "internal.evc.item.ttl";

    /*
     * Internal pool config values
     */
    public static final String POOL_READ_INSTANCES                  = "readInstances";
    public static final String POOL_WRITE_INSTANCES                 = "writeInstances";
    public static final String POOL_RECONCILE                       = "reconcile";
    public static final String POOL_CHANGED                         = "asgChanged";
    public static final String POOL_SERVERGROUP_STATUS              = "asgStatus";
    public static final String POOL_READ_Q_SIZE                     = "readQueue";
    public static final String POOL_WRITE_Q_SIZE                    = "writeQueue";
    public static final String POOL_REFRESH_QUEUE_FULL              = "refreshOnQueueFull";
    public static final String POOL_REFRESH_ASYNC                   = "refreshAsync";
    public static final String POOL_OPERATIONS                      = "operations";


    /**
     * Metric Tags Names
     */
    public static final String CACHE                            = "ipc.server.app";
    public static final String SERVERGROUP                      = "ipc.server.asg";
    public static final String ZONE                             = "ipc.server.zone";
    public static final String ATTEMPT                          = "ipc.attempt";
    public static final String IPC_RESULT                       = "ipc.result";
    public static final String IPC_STATUS                       = "ipc.status";
    //public static final String FAIL_REASON                      = "ipc.error.group";

    /*
     * Metric Tags moved to IPC format
     */
    public static final String CALL_TAG                         = "evc.call";
    public static final String CALL_TYPE_TAG                    = "evc.call.type";
    public static final String CACHE_HIT                        = "evc.cache.hit";
    public static final String CONNECTION_ID                    = "evc.connection.id";
    public static final String TTL                              = "evc.ttl";

    public static final String PAUSE_REASON                     = "evc.pause.reason";
    public static final String LATCH                            = "evc.latch";
    public static final String FAIL_COUNT                       = "evc.fail.count";
    public static final String COMPLETE_COUNT                   = "evc.complete.count";
    public static final String RECONNECT_COUNT                  = "evc.reconnect.count";
    public static final String FETCH_AFTER_PAUSE                = "evc.fetch.after.pause";
    public static final String FAILED_SERVERGROUP               = "evc.failed.asg";
    public static final String CONFIG_NAME                      = "evc.config";
    public static final String STAT_NAME                        = "evc.stat.name";
    public static final String FAILED_HOST                      = "evc.failed.host";
    public static final String OPERATION                        = "evc.operation";
    public static final String OPERATION_STATUS                 = "evc.operation.status";
    public static final String NUMBER_OF_ATTEMPTS               = "evc.attempts";
    public static final String NUMBER_OF_KEYS                   = "evc.keys.count";
    public static final String METRIC                           = "evc.metric";
    public static final String FAILURE_REASON                   = "evc.fail.reason";
    public static final String PREFIX                           = "evc.prefix";
    public static final String EVENT                            = "evc.event";
    public static final String EVENT_STAGE                      = "evc.event.stage";
    public static final String CONNECTION                       = "evc.connection.type";
    public static final String TLS                              = "evc.connection.tls";

    /**
     * Metric Tags Values  
     */
    public static final String SIZE                             = "size";
    public static final String PORT                             = "port";
    public static final String CONNECT                          = "connect";
    public static final String DISCONNECT                       = "disconnect";
    public static final String SUCCESS                          = "success";
    public static final String FAIL                             = "failure";
    public static final String TIMEOUT                          = "timeout";
    public static final String CHECKED_OP_TIMEOUT               = "CheckedOperationTimeout";
    public static final String CANCELLED                        = "cancelled";
    public static final String THROTTLED                        = "throttled";
    public static final String ERROR                            = "error";
    public static final String READ                             = "read";
    public static final String WRITE                            = "write";
    public static final String YES                              = "yes";
    public static final String NO                               = "no";
    public static final String PARTIAL                          = "partial";
    public static final String UNKNOWN                          = "unknown";
    public static final String INTERRUPTED                      = "interrupted";
    public static final String SCHEDULE                         = "Scheduling";
    public static final String GC                               = "gc";
    public static final String NULL_CLIENT                      = "nullClient";
    public static final String INVALID_TTL                      = "invalidTTL";
    public static final String NULL_ZONE                        = "nullZone";
    public static final String NULL_SERVERGROUP                 = "nullASG";
    public static final String RECONNECT                        = "reconnect";
    public static final String CALLBACK                         = "callback";
    public static final String VERIFY                           = "verify";
    public static final String READ_QUEUE_FULL                  = "readQueueFull";
    public static final String INACTIVE_NODE                    = "inactiveNode";
    public static final String IGNORE_INACTIVE_NODES            = "ignoreInactiveNode";
    public static final String INCORRECT_CHUNKS                 = "incorrectNumOfChunks";
    public static final String INVALID_CHUNK_SIZE               = "invalidChunkSize";
    public static final String CHECK_SUM_ERROR                  = "checkSumError";
    public static final String KEY_HASH_COLLISION               = "KeyHashCollision";
    public static final String NUM_CHUNK_SIZE                   = "numOfChunks";
    public static final String CHUNK_DATA_SIZE                  = "dataSize";
    public static final String NOT_AVAILABLE                    = "notAvailable";
    public static final String NOT_ACTIVE                       = "notActive";

    public static final String INITIAL                          = "initial";
    public static final String SECOND                           = "second";
    public static final String THIRD_UP                         = "third_up";
    /**
     * Metric Tag Value for Operations
     */
    public static final String BULK_OPERATION                   = "BULK";
    public static final String GET_OPERATION                    = "GET";
    public static final String GET_AND_TOUCH_OPERATION          = "GET_AND_TOUCH";
    public static final String DELETE_OPERATION                 = "DELETE";
    public static final String TOUCH_OPERATION                  = "TOUCH";
    public static final String AOA_OPERATION                    = "APPEND_OR_ADD";
    public static final String AOA_OPERATION_APPEND             = "APPEND_OR_ADD-APPEND";
    public static final String AOA_OPERATION_ADD                = "APPEND_OR_ADD-ADD";
    public static final String AOA_OPERATION_REAPPEND           = "APPEND_OR_ADD-RETRY-APPEND";
    public static final String SET_OPERATION                    = "SET";
    public static final String ADD_OPERATION                    = "ADD";
    public static final String REPLACE_OPERATION                = "REPLACE";

    public static final String META_GET_OPERATION               = "M_GET";
    public static final String META_SET_OPERATION               = "M_SET";
    public static final String META_DEBUG_OPERATION             = "M_DEBUG";

}
