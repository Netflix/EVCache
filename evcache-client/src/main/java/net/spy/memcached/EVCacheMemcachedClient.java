package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicLongProperty;
import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheBulkGetFuture;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.monitor.Timer;
import com.netflix.spectator.api.DistributionSummary;

import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.GetAndTouchOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Mutator;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.protocol.binary.EVCacheNodeImpl;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
"SIC_INNER_SHOULD_BE_STATIC_ANON" })
public class EVCacheMemcachedClient extends MemcachedClient {

    private static final Logger log = LoggerFactory.getLogger(EVCacheMemcachedClient.class);
    private final int id;
    private final String appName;
    private final String zone;
    private final ChainedDynamicProperty.IntProperty readTimeout;
    private final ServerGroup serverGroup;
    private final EVCacheClient client;
    private final ConnectionFactory connectionFactory;
    private final Map<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<String, Timer>();

    private DistributionSummary getDataSize, bulkDataSize, getAndTouchDataSize;
    private DynamicLongProperty mutateOperationTimeout;

    public EVCacheMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs,
            ChainedDynamicProperty.IntProperty readTimeout, String appName, String zone, int id,
            ServerGroup serverGroup, EVCacheClient client) throws IOException {
        super(cf, addrs);
        this.connectionFactory = cf;
        this.id = id;
        this.appName = appName;
        this.zone = zone;
        this.readTimeout = readTimeout;
        this.serverGroup = serverGroup;
        this.client = client;
    }

    public NodeLocator getNodeLocator() {
        return this.mconn.getLocator();
    }

    public MemcachedNode getEVCacheNode(String key) {
        return this.mconn.getLocator().getPrimary(key);
    }

    public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
        throw new UnsupportedOperationException("asyncGet");
    }

    public <T> EVCacheOperationFuture<T> asyncGet(final String key, final Transcoder<T> tc, EVCacheGetOperationListener<T> listener) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EVCacheOperationFuture<T> rv = new EVCacheOperationFuture<T>(key, latch, new AtomicReference<T>(null), readTimeout.get().intValue(), executorService, appName, serverGroup);
        final Stopwatch operationDuration = getTimer(GET_OPERATION_STRING).start();
        Operation op = opFact.get(key, new GetOperation.Callback() {
            private Future<T> val = null;

            public void receivedStatus(OperationStatus status) {
                operationDuration .stop();
                try {
                    if (val != null) {
                        rv.set(val.get(), status);
                    } else {
                        rv.set(null, status);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    rv.set(null, status);
                }
            }

            @SuppressWarnings("unchecked")
            public void gotData(String k, int flags, byte[] data) {

                if (data != null)  {
                    if(getDataSize == null) getDataSize = EVCacheMetricsFactory.getDistributionSummary(appName + "-GetOperation-DataSize", appName, serverGroup.getName());
                    if (getDataSize != null) getDataSize.record(data.length);
                }
                if (!key.equals(k)) log.warn("Wrong key returned. Key - {}; Returned Key {}", key, k);
                if (tc == null) {
                    if (tcService == null) {
                        log.error("tcService is null, will not be able to decode");
                        throw new RuntimeException("TranscoderSevice is null. Not able to decode");
                    } else {
                        final Transcoder<T> t = (Transcoder<T>) getTranscoder();
                        val = tcService.decode(t, new CachedData(flags, data, t.getMaxSize()));
                    }
                } else {
                    if (tcService == null) {
                        log.error("tcService is null, will not be able to decode");
                        throw new RuntimeException("TranscoderSevice is null. Not able to decode");
                    } else {
                        val = tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize()));
                    }
                }
            }

            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (listener != null) rv.addListener(listener);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> EVCacheBulkGetFuture<T> asyncGetBulk(Collection<String> keys, final Transcoder<T> tc, EVCacheGetOperationListener<T> listener, String metricName) {
        final Map<String, Future<T>> m = new ConcurrentHashMap<String, Future<T>>();

        // Break the gets down into groups by key
        final Map<MemcachedNode, Collection<String>> chunks = new HashMap<MemcachedNode, Collection<String>>();
        final NodeLocator locator = mconn.getLocator();

        final Iterator<String> keyIter = keys.iterator();
        while (keyIter.hasNext()) {
            final String key = keyIter.next();
            StringUtils.validateKey(key, opFact instanceof BinaryOperationFactory);
            final MemcachedNode primaryNode = locator.getPrimary(key);
            if (primaryNode.isActive()) {
                Collection<String> ks = chunks.get(primaryNode);
                if (ks == null) {
                    ks = new ArrayList<String>();
                    chunks.put(primaryNode, ks);
                }
                ks.add(key);
            }
        }

        final AtomicInteger pendingChunks = new AtomicInteger(chunks.size());
        int initialLatchCount = chunks.isEmpty() ? 0 : 1;
        final CountDownLatch latch = new CountDownLatch(initialLatchCount);
        final Collection<Operation> ops = new ArrayList<Operation>(chunks.size());
        final EVCacheBulkGetFuture<T> rv = new EVCacheBulkGetFuture<T>(appName, m, ops, latch, executorService, serverGroup, metricName);
        final Stopwatch operationDuration = getTimer(BULK_OPERATION_STRING).start(); 
        GetOperation.Callback cb = new GetOperation.Callback() {
            @Override
            @SuppressWarnings("synthetic-access")
            public void receivedStatus(OperationStatus status) {
                operationDuration.stop();
                rv.setStatus(status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
                if (data != null)  {
                    if(bulkDataSize == null) bulkDataSize = EVCacheMetricsFactory.getDistributionSummary(appName + "-BulkOperation-DataSize", appName, serverGroup.getName());
                    if (bulkDataSize != null) bulkDataSize.record(data.length);
                }
                m.put(k, tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize())));
            }

            @Override
            public void complete() {
                if (pendingChunks.decrementAndGet() <= 0) {
                    latch.countDown();
                    rv.signalComplete();
                }
            }
        };

        // Now that we know how many servers it breaks down into, and the latch
        // is all set up, convert all of these strings collections to operations
        final Map<MemcachedNode, Operation> mops = new HashMap<MemcachedNode, Operation>();

        for (Map.Entry<MemcachedNode, Collection<String>> me : chunks.entrySet()) {
            Operation op = opFact.get(me.getValue(), cb);
            mops.put(me.getKey(), op);
            ops.add(op);
        }
        assert mops.size() == chunks.size();
        mconn.checkState();
        mconn.addOperations(mops);
        return rv;
    }

    public <T> EVCacheOperationFuture<CASValue<T>> asyncGetAndTouch(final String key, final int exp, final Transcoder<T> tc) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EVCacheOperationFuture<CASValue<T>> rv = new EVCacheOperationFuture<CASValue<T>>(key, latch, new AtomicReference<CASValue<T>>(null), connectionFactory.getOperationTimeout(), executorService, appName, serverGroup);
        final Stopwatch operationDuration = getTimer(GET_AND_TOUCH_OPERATION_STRING).start();
        Operation op = opFact.getAndTouch(key, exp, new GetAndTouchOperation.Callback() {
            private CASValue<T> val = null;

            public void receivedStatus(OperationStatus status) {
                operationDuration.stop();
                rv.set(val, status);
            }

            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }

            public void gotData(String k, int flags, long cas, byte[] data) {
                if (!key.equals(k)) log.warn("Wrong key returned. Key - {}; Returned Key {}", key, k);
                if (data != null)  {
                    if(getAndTouchDataSize == null) getAndTouchDataSize = EVCacheMetricsFactory.getDistributionSummary(appName + "-GATOperation-DataSize", appName, serverGroup.getName());
                    if (getAndTouchDataSize != null) getAndTouchDataSize.record(data.length);
                }

                val = new CASValue<T>(cas, tc.decode(new CachedData(flags, data, tc.getMaxSize())));
            }
        });
        rv.setOperation(op);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> OperationFuture<Boolean> set(String key, int exp, T o, final Transcoder<T> tc) {
        return asyncStore(StoreType.set, key, exp, o, tc, null);
    }

    public OperationFuture<Boolean> set(String key, int exp, Object o) {
        return asyncStore(StoreType.set, key, exp, o, transcoder, null);
    }

    @SuppressWarnings("unchecked")
    public <T> OperationFuture<Boolean> set(String key, int exp, T o, final Transcoder<T> tc, EVCacheLatch latch) {
        Transcoder<T> t = (Transcoder<T>) ((tc == null) ? transcoder : tc);
        return asyncStore(StoreType.set, key, exp, o, t, latch);
    }

    @SuppressWarnings("unchecked")
    public <T> OperationFuture<Boolean> replace(String key, int exp, T o, final Transcoder<T> tc, EVCacheLatch latch) {
        Transcoder<T> t = (Transcoder<T>) ((tc == null) ? transcoder : tc);
        return asyncStore(StoreType.replace, key, exp, o, t, latch);
    }

    public <T> OperationFuture<Boolean> add(String key, int exp, T o, Transcoder<T> tc) {
        return asyncStore(StoreType.add, key, exp, o, tc, null);
    }

    public OperationFuture<Boolean> delete(String key, EVCacheLatch evcacheLatch) {
        final CountDownLatch latch = new CountDownLatch(1);
        final OperationFuture<Boolean> rv = new OperationFuture<Boolean>(key, latch, connectionFactory.getOperationTimeout(), executorService);
        final Stopwatch operationDuration = getTimer(DELETE_STRING).start();
        final DeleteOperation.Callback callback = new DeleteOperation.Callback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                operationDuration.stop();
                rv.set(Boolean.TRUE, status);
                if (status.getStatusCode().equals(StatusCode.SUCCESS)) {
                    getCounter(DELETE_OPERATION_SUCCESS_STRING).increment();
                } else {
                    getCounter("DeleteOperation-"+ status.getStatusCode().name()).increment();
                }
            }

            @Override
            public void gotData(long cas) {
                rv.setCas(cas);
            }

            @Override
            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        };

        final DeleteOperation op = opFact.delete(key, callback);
        rv.setOperation(op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> OperationFuture<Boolean> touch(final String key, final int exp, EVCacheLatch evcacheLatch) {
        final CountDownLatch latch = new CountDownLatch(1);
        final OperationFuture<Boolean> rv = new OperationFuture<Boolean>(key, latch, connectionFactory.getOperationTimeout(), executorService);
        final Stopwatch operationDuration = getTimer(TOUCH_OPERATION_STRING).start();
        Operation op = opFact.touch(key, exp, new OperationCallback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                operationDuration.stop();
                rv.set(status.isSuccess(), status);

                if (status.getStatusCode().equals(StatusCode.SUCCESS)) {
                    getCounter(TOUCH_OPERATION_SUCCESS_STRING).increment();
                } else {
                    getCounter(TOUCH_OPERATION_STRING + "-" + status.getStatusCode().name()).increment();
                }
            }

            @Override
            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }


    public <T> OperationFuture<Boolean> asyncAppendOrAdd(final String key, int exp, CachedData co, EVCacheLatch evcacheLatch) {
        final CountDownLatch latch = new CountDownLatch(1);
        final OperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), connectionFactory.getOperationTimeout(), executorService, appName, serverGroup);
        final Stopwatch operationDuration = getTimer(AOA_STRING).start();
        Operation op = opFact.cat(ConcatenationType.append, 0, key, co.getData(),
                new OperationCallback() {
            boolean appendSuccess = false;
            @Override
            public void receivedStatus(OperationStatus val) {
                if (val.getStatusCode().equals(StatusCode.SUCCESS)) {
                    if (log.isDebugEnabled()) log.debug("AddOrAppend Key (Append Operation): " + key + "; Status : " + val.getStatusCode().name()
                            + "; Message : " + val.getMessage() + "; Elapsed Time - " + (operationDuration.getDuration(TimeUnit.MILLISECONDS)));

                    getCounter(AOA_APPEND_OPERATION_SUCCESS_STRING).increment();
                    rv.set(Boolean.TRUE, val);
                    appendSuccess = true;
                } else {
                    getCounter("AoA-AppendOperation-FAIL").increment();
                }
            }

            @Override
            public void complete() {
                if(appendSuccess)  {
                    operationDuration.stop();
                    latch.countDown();
                    rv.signalComplete();
                } else {
                    Operation op = opFact.store(StoreType.add, key, co.getFlags(), exp, co.getData(), new StoreOperation.Callback() {
                        @Override
                        public void receivedStatus(OperationStatus addStatus) {
                            if (log.isDebugEnabled()) log.debug("AddOrAppend Key (Ad Operation): " + key + "; Status : " + addStatus.getStatusCode().name()
                                    + "; Message : " + addStatus.getMessage() + "; Elapsed Time - " + (operationDuration.getDuration(TimeUnit.MILLISECONDS)));
                            if(addStatus.isSuccess()) {
                                rv.set(Boolean.TRUE, addStatus);
                                appendSuccess = true;
                                getCounter(AOA_ADD_OPERATION_SUCCESS_STRING).increment();
                            } else {
                                getCounter("AoA-AddOperation-FAIL").increment();
                                Operation op = opFact.cat(ConcatenationType.append, 0, key, co.getData(),
                                        new OperationCallback() {
                                    public void receivedStatus(OperationStatus retryAppendStatus) {
                                        if (retryAppendStatus.getStatusCode().equals(StatusCode.SUCCESS)) {
                                            rv.set(Boolean.TRUE, retryAppendStatus);
                                            if (log.isDebugEnabled()) log.debug("AddOrAppend Retry append Key (Append Operation): " + key + "; Status : " + retryAppendStatus.getStatusCode().name()
                                                    + "; Message : " + retryAppendStatus.getMessage() + "; Elapsed Time - " + (operationDuration.getDuration(TimeUnit.MILLISECONDS)));

                                            getCounter("AoA-RetryAppendOperation-SUCCESS").increment();
                                        } else {
                                            rv.set(Boolean.FALSE, retryAppendStatus);
                                            getCounter("AoA-RetryAppendOperation-FAIL").increment();
                                        }
                                    }
                                    public void complete() {
                                        operationDuration.stop();
                                        latch.countDown();
                                        rv.signalComplete();
                                    }
                                });
                                rv.setOperation(op);
                                mconn.enqueueOperation(key, op);
                            }
                        }

                        @Override
                        public void gotData(String key, long cas) {
                            rv.setCas(cas);
                        }

                        @Override
                        public void complete() {
                            if(appendSuccess) {
                                operationDuration.stop();
                                latch.countDown();
                                rv.signalComplete();
                            }
                        }
                    });
                    rv.setOperation(op);
                    mconn.enqueueOperation(key, op);
                }
            }
        });
        rv.setOperation(op);
        mconn.enqueueOperation(key, op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        return rv;
    }

    private Timer getTimer(String name) {
        Timer timer = timerMap.get(name);
        if(timer != null) return timer;

        timer = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, name);
        timerMap.put(name, timer);
        return timer;
    }

    private Counter getCounter(String counterMetric) {
        Counter counter = counterMap.get(counterMetric);
        if(counter != null) return counter;

        counter = EVCacheMetricsFactory.getCounter(appName, null, serverGroup.getName(), appName + "-" + counterMetric, DataSourceType.COUNTER);
        counterMap.put(counterMetric, counter);
        return counter;
    }

    private <T> OperationFuture<Boolean> asyncStore(final StoreType storeType, final String key, int exp, T value, Transcoder<T> tc, EVCacheLatch evcacheLatch) {
        final CachedData co;
        if (value instanceof CachedData) {
            co = (CachedData) value;
        } else {
            co = tc.encode(value);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final String operationStr;
        final String operationSuccessStr;
        if (storeType == StoreType.set) {
            operationStr = SET_OPERATION_STRING;
            operationSuccessStr = SET_OPERATION_SUCCESS_STRING;
        } else if (storeType == StoreType.add) {
            operationStr = ADD_OPERATION_STRING;
            operationSuccessStr = ADD_OPERATION_SUCCESS_STRING;
        } else {
            operationStr = REPLACE_OPERATION_STRING;
            operationSuccessStr = REPLACE_OPERATION_SUCCESS_STRING;
        }

        final Timer timer = getTimer(operationStr);
        final OperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), connectionFactory.getOperationTimeout(), executorService, appName, serverGroup);
        Operation op = opFact.store(storeType, key, co.getFlags(), exp, co.getData(), new StoreOperation.Callback() {
            
            final Stopwatch operationDuration = timer.start();

            @Override
            public void receivedStatus(OperationStatus val) {
                operationDuration.stop();
                if (log.isDebugEnabled()) log.debug("Storing Key : " + key + "; Status : " + val.getStatusCode().name()
                        + "; Message : " + val.getMessage() + "; Elapsed Time - " + operationDuration.getDuration(TimeUnit.MILLISECONDS));
                if (val.getStatusCode().equals(StatusCode.SUCCESS)) {
                    getCounter(operationSuccessStr).increment();
                } else {
                    if (val.getStatusCode().equals(StatusCode.TIMEDOUT)) {
                        getCounter(operationStr + "-TIMEDOUT").increment();
                        if (log.isInfoEnabled()) log.info("Timedout Storing Key : " + key + "; Status : " + val.getStatusCode().name()
                                + "; Message : " + val.getMessage() + "; Elapsed Time - " + operationDuration.getDuration(TimeUnit.MILLISECONDS), new Exception());
                    } else {
                        if (log.isInfoEnabled()) log.info(val.getStatusCode().name() + " Storing Key : " + key + "; Status : " + val.getStatusCode().name()
                                + "; Message : " + val.getMessage() + "; Elapsed Time - " + operationDuration.getDuration(TimeUnit.MILLISECONDS), new Exception());
                        getCounter(operationStr + "-" + val.getStatusCode().name()).increment();
                    }
                }
                rv.set(val.isSuccess(), val);
            }

            @Override
            public void gotData(String key, long cas) {
                rv.setCas(cas);
            }

            @Override
            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public String toString() {
        return appName + "_" + zone + " _" + id;
    }

    @SuppressWarnings("unchecked")
    public <T> OperationFuture<Boolean> add(String key, int exp, T o, final Transcoder<T> tc, EVCacheLatch latch) {
        Transcoder<T> t = (Transcoder<T>) ((tc == null) ? transcoder : tc);
        return asyncStore(StoreType.add, key, exp, o, t, latch);
    }

    public long incr(String key, long by, long def, int exp) {
        final Stopwatch operationDuration = getTimer(INCR_OPERATION_STRING).start();
        long val = 0;
        try {
            val = mutate(Mutator.incr, key, by, def, exp);
        } finally {
            operationDuration.stop();
            if (log.isDebugEnabled()) log.debug("Increment Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp 
                    + "; val : " + val + "; Elapsed Time - " + operationDuration.getDuration(TimeUnit.MILLISECONDS));
        }
        return val;
    }


    public long decr(String key, long by, long def, int exp) {
        final Stopwatch operationDuration = getTimer(DECR_OPERATION_STRING).start();
        long val = 0;
        try {
            val = super.decr(key, by, def, exp);
        } finally {
            operationDuration.stop();
            if (log.isDebugEnabled()) log.debug("decrement Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp 
                    + "; val : " + val + "; Elapsed Time - " + (operationDuration.getDuration(TimeUnit.MILLISECONDS)));
        }
        return val;
    }

    public long mutate(Mutator m, String key, long by, long def, int exp) {
        final AtomicLong rv = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(1);
        mconn.enqueueOperation(key, opFact.mutate(m, key, by, def, exp, new OperationCallback() {
            @Override
            public void receivedStatus(OperationStatus s) {
                rv.set(new Long(s.isSuccess() ? s.getMessage() : "-1"));
            }

            @Override
            public void complete() {
                latch.countDown();
            }
        }));
        try {
            if(mutateOperationTimeout == null) {
                mutateOperationTimeout = EVCacheConfig.getInstance().getDynamicLongProperty("evache.mutate.timeout", connectionFactory.getOperationTimeout());
            }

            if (!latch.await(mutateOperationTimeout.get(), TimeUnit.MILLISECONDS)) {
                return rv.get();
            }
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);

        }
        getLogger().debug("Mutation returned " + rv);
        return rv.get();
    }

    public void reconnectNode(EVCacheNodeImpl evcNode ) {
        final long upTime = System.currentTimeMillis() - evcNode.getCreateTime();
        if (log.isDebugEnabled()) log.debug("Reconnecting node : " + evcNode + "; UpTime : " + upTime);
        if(upTime > 30000) { //not more than once every 30 seconds : TODO make this configurable
            EVCacheMetricsFactory.getCounter(appName + "-RECONNECT", evcNode.getBaseTags()).increment();
            evcNode.setConnectTime(System.currentTimeMillis());
            mconn.queueReconnect(evcNode);
        }
    }
    private final String BULK_OPERATION_STRING = "BulkOperation";
    private final String GET_OPERATION_STRING = "GetOperation";
    private final String GET_AND_TOUCH_OPERATION_STRING = "GetAndTouchOperation";
    private final String DELETE_STRING = "DeleteOperation";
    private final String TOUCH_OPERATION_STRING = "TouchOperation";
    private final String TOUCH_OPERATION_SUCCESS_STRING = "TouchOperation-SUCCESS";
    private final String AOA_STRING = "AoAOperation";
    private final String SET_OPERATION_STRING = "SetOperation";
    private final String ADD_OPERATION_STRING = "AddOperation";
    private final String REPLACE_OPERATION_STRING = "ReplaceOperation";
    private final String INCR_OPERATION_STRING = "IncrOperation";
    private final String DECR_OPERATION_STRING = "DecrOperation";
    private final String DELETE_OPERATION_SUCCESS_STRING = "DeleteOperation-SUCCESS";
    private final String SET_OPERATION_SUCCESS_STRING = "SetOperation-SUCCESS";
    private final String ADD_OPERATION_SUCCESS_STRING = "AddOperation-SUCCESS";
    private final String REPLACE_OPERATION_SUCCESS_STRING = "ReplaceOperation-SUCCESS";
    private final String AOA_APPEND_OPERATION_SUCCESS_STRING = "AoA-AppendOperation-SUCCESS";
    private final String AOA_ADD_OPERATION_SUCCESS_STRING = "AoA-AddOperation-SUCCESS";

}
