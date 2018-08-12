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
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;

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
    private final String appName;
    private final ChainedDynamicProperty.IntProperty readTimeout;
    private final EVCacheClient client;
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<String, Timer>();
    private final Map<String, DistributionSummary> distributionSummaryMap = new ConcurrentHashMap<String, DistributionSummary>();

    private DynamicLongProperty mutateOperationTimeout;
    private final ConnectionFactory connectionFactory;

    public EVCacheMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs,
            ChainedDynamicProperty.IntProperty readTimeout, EVCacheClient client) throws IOException {
        super(cf, addrs);
        this.connectionFactory = cf;
        this.readTimeout = readTimeout;
        this.client = client;
        this.appName = client.getAppName();
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
        final EVCacheOperationFuture<T> rv = new EVCacheOperationFuture<T>(key, latch, new AtomicReference<T>(null), readTimeout.get().intValue(), executorService, client);
        final Operation op = opFact.get(key, new GetOperation.Callback() {
            private Future<T> val = null;

            public void receivedStatus(OperationStatus status) {
                if (log.isDebugEnabled()) log.debug("Getting Key : " + key + "; Status : " + status.getStatusCode().name() + (log.isTraceEnabled() ?  " Node : " + getEVCacheNode(key) : "")
                        + "; Message : " + status.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                try {
                    if (val != null) {
                        if (log.isTraceEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.trace("Key : " + key + "; val : " + val.get());
                        rv.set(val.get(), status);
                    } else {
                        if (log.isTraceEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.trace("Key : " + key + "; val is null");
                        rv.set(null, status);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    rv.set(null, status);
                }
            }

            @SuppressWarnings("unchecked")
            public void gotData(String k, int flags, byte[] data) {

                if (!key.equals(k)) {
                    log.error("Wrong key returned. Key - " + key + "; Returned Key " + k);
                    return;
                }
                if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Read data : key " + key + "; flags : " + flags + "; data : " + data);
                if (data != null)  {
                    if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Key : " + key + "; val size : " + data.length);
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.GET_OPERATION, EVCacheMetricsFactory.READ).record(data.length);
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
                } else {
                    if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Key : " + key + "; val is null" );
                }
            }

            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                getTimer(EVCacheMetricsFactory.GET_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (val != null ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (listener != null) rv.addListener(listener);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> EVCacheBulkGetFuture<T> asyncGetBulk(Collection<String> keys, final Transcoder<T> tc, EVCacheGetOperationListener<T> listener) {
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
        final EVCacheBulkGetFuture<T> rv = new EVCacheBulkGetFuture<T>(m, ops, latch, executorService, client);
        GetOperation.Callback cb = new GetOperation.Callback() {
            @Override
            @SuppressWarnings("synthetic-access")
            public void receivedStatus(OperationStatus status) {
                if (log.isDebugEnabled()) log.debug("GetBulk Keys : " + keys + "; Status : " + status.getStatusCode().name() + "; Message : " + status.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                rv.setStatus(status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
                if (data != null)  {
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.BULK_OPERATION, EVCacheMetricsFactory.READ).record(data.length);
                }
                m.put(k, tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize())));
            }

            @Override
            public void complete() {
                if (pendingChunks.decrementAndGet() <= 0) {
                    latch.countDown();
                    getTimer(EVCacheMetricsFactory.BULK_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (m.size() == keys.size() ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), null).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
        final EVCacheOperationFuture<CASValue<T>> rv = new EVCacheOperationFuture<CASValue<T>>(key, latch, new AtomicReference<CASValue<T>>(null), operationTimeout, executorService, client);
        Operation op = opFact.getAndTouch(key, exp, new GetAndTouchOperation.Callback() {
            private CASValue<T> val = null;

            public void receivedStatus(OperationStatus status) {
                if (log.isDebugEnabled()) log.debug("GetAndTouch Key : " + key + "; Status : " + status.getStatusCode().name()
                        + (log.isTraceEnabled() ?  " Node : " + getEVCacheNode(key) : "")
                        + "; Message : " + status.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                rv.set(val, status);
            }

            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                getTimer(EVCacheMetricsFactory.GET_AND_TOUCH_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (val != null ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }

            public void gotData(String k, int flags, long cas, byte[] data) {
                if (!key.equals(k)) log.warn("Wrong key returned. Key - " + key + "; Returned Key " + k);
                if (data != null) getDataSizeDistributionSummary(EVCacheMetricsFactory.GET_AND_TOUCH_OPERATION, EVCacheMetricsFactory.READ).record(data.length);
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
        final EVCacheOperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), operationTimeout, executorService, client);
        final DeleteOperation op = opFact.delete(key, new DeleteOperation.Callback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                rv.set(Boolean.TRUE, status);
            }

            @Override
            public void gotData(long cas) {
                rv.setCas(cas);
            }

            @Override
            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                getTimer(EVCacheMetricsFactory.DELETE_OPERATION, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }
        });

        rv.setOperation(op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> OperationFuture<Boolean> touch(final String key, final int exp, EVCacheLatch evcacheLatch) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EVCacheOperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), operationTimeout, executorService, client);
        final Operation op = opFact.touch(key, exp, new OperationCallback() {
            @Override
            public void receivedStatus(OperationStatus status) {
                rv.set(status.isSuccess(), status);
            }

            @Override
            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                getTimer(EVCacheMetricsFactory.TOUCH_OPERATION, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
        final EVCacheOperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), operationTimeout, executorService, client);
        final Operation opAppend = opFact.cat(ConcatenationType.append, 0, key, co.getData(), new OperationCallback() {
            boolean appendSuccess = false;
            @Override
            public void receivedStatus(OperationStatus val) {
                if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("AddOrAppend Key (Append Operation): " + key + "; Status : " + val.getStatusCode().name()
                        + "; Message : " + val.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                if (val.getStatusCode().equals(StatusCode.SUCCESS)) {
                    rv.set(Boolean.TRUE, val);
                    appendSuccess = true;
                    
                }
            }

            @Override
            public void complete() {
                if(appendSuccess)  {
                    final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                    getTimer(EVCacheMetricsFactory.AOA_OPERATION_APPEND, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host);
                    latch.countDown();
                    rv.signalComplete();
                } else {
                    Operation opAdd = opFact.store(StoreType.add, key, co.getFlags(), exp, co.getData(), new StoreOperation.Callback() {
                        @Override
                        public void receivedStatus(OperationStatus addStatus) {
                            if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("AddOrAppend Key (Add Operation): " + key + "; Status : " + addStatus.getStatusCode().name()
                                    + "; Message : " + addStatus.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                            if(addStatus.isSuccess()) {
                                appendSuccess = true;
                                rv.set(addStatus.isSuccess(), addStatus);
                            } else {
                                Operation opReappend = opFact.cat(ConcatenationType.append, 0, key, co.getData(), new OperationCallback() {
                                    public void receivedStatus(OperationStatus retryAppendStatus) {
                                        if (retryAppendStatus.getStatusCode().equals(StatusCode.SUCCESS)) {
                                            rv.set(Boolean.TRUE, retryAppendStatus);
                                            if (log.isDebugEnabled()) log.debug("AddOrAppend Retry append Key (Append Operation): " + key + "; Status : " + retryAppendStatus.getStatusCode().name()
                                                    + "; Message : " + retryAppendStatus.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                                        } else {
                                            rv.set(Boolean.FALSE, retryAppendStatus);
                                        }
                                    }
                                    public void complete() {
                                        final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                                        getTimer(EVCacheMetricsFactory.AOA_OPERATION_REAPPEND, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                                        latch.countDown();
                                        rv.signalComplete();
                                    }
                                });
                                rv.setOperation(opReappend);
                                mconn.enqueueOperation(key, opReappend);
                            }
                        }

                        @Override
                        public void gotData(String key, long cas) {
                            rv.setCas(cas);
                        }

                        @Override
                        public void complete() {
                            if(appendSuccess) {
                                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                                getTimer(EVCacheMetricsFactory.AOA_OPERATION_ADD, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                                latch.countDown();
                                rv.signalComplete();
                            }
                        }
                    });
                    rv.setOperation(opAdd);
                    mconn.enqueueOperation(key, opAdd);
                }
            }
        });
        rv.setOperation(opAppend);
        mconn.enqueueOperation(key, opAppend);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        return rv;
    }

//            private Timer getTimer(String operation, String operationType, OperationStatus status, String hit) {
//                return getTimer(operation, operationType, status, hit, null);
//            }

    private Timer getTimer(String operation, String operationType, OperationStatus status, String hit, String host) {
        String name = ((status != null) ? operation + status.getMessage() : operation );
        if(hit != null) name = name + hit;
    
        Timer timer = timerMap.get(name);
        if(timer != null) return timer; 

        final List<Tag> tagList = new ArrayList<Tag>(client.getTagList().size() + 4 + (host == null ? 0 : 1));
        tagList.addAll(client.getTagList());
        if(operation != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION, operation));
        if(operationType != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_TYPE, operationType));
        if(status != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_STATUS, status.getMessage()));
        if(hit != null) tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE_HIT, hit));
        if(host != null) tagList.add(new BasicTag(EVCacheMetricsFactory.HOST, host));

        timer = EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.INTERNAL_CALL, tagList);
        timerMap.put(name, timer);
        return timer;
    }

//    private void increment(String operation, String operationType, OperationStatus status, String hit) {
//        final List<Tag> tagList = new ArrayList<Tag>(6);
//        tagList.addAll(client.getTagList());
//        if(operation != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION, operation));
//        if(operationType != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_TYPE, operationType));
//        if(status != null) tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_STATUS, status.getMessage()));
//        if(hit != null) tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE_HIT, hit));
//
//        EVCacheMetricsFactory.getInstance().getCounter(EVCacheMetricsFactory.INTERNAL_CALL, tagList).increment();
//    }

    private DistributionSummary getDataSizeDistributionSummary(String operation, String type) {
        DistributionSummary distributionSummary = distributionSummaryMap.get(operation);
        if(distributionSummary != null) return distributionSummary;

        final List<Tag> tagList = new ArrayList<Tag>(6);
        tagList.addAll(client.getTagList());
        tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION, operation));
        tagList.add(new BasicTag(EVCacheMetricsFactory.OPERATION_TYPE, type));
        distributionSummary = EVCacheMetricsFactory.getInstance().getDistributionSummary(EVCacheMetricsFactory.DATA_SIZE, tagList);
        distributionSummaryMap.put(operation, distributionSummary);
        return distributionSummary;
    }

//    private Counter getCounter(String counterMetric) {
//        return getCounter(counterMetric, null);
//    }

    private <T> OperationFuture<Boolean> asyncStore(final StoreType storeType, final String key, int exp, T value, Transcoder<T> tc, EVCacheLatch evcacheLatch) {
        final CachedData co;
        if (value instanceof CachedData) {
            co = (CachedData) value;
        } else {
            co = tc.encode(value);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final String operationStr;
        if (storeType == StoreType.set) {
            operationStr = EVCacheMetricsFactory.SET_OPERATION;
        } else if (storeType == StoreType.add) {
            operationStr = EVCacheMetricsFactory.ADD_OPERATION;
        } else {
            operationStr = EVCacheMetricsFactory.REPLACE_OPERATION;
        }

        final EVCacheOperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), operationTimeout, executorService, client);
        final Operation op = opFact.store(storeType, key, co.getFlags(), exp, co.getData(), new StoreOperation.Callback() {
            @Override
            public void receivedStatus(OperationStatus val) {
                if (log.isDebugEnabled()) log.debug("Storing Key : " + key + "; Status : " + val.getStatusCode().name() + (log.isTraceEnabled() ?  " Node : " + getEVCacheNode(key) : "") + "; Message : " + val.getMessage()
                + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                rv.set(val.isSuccess(), val);
                if (log.isTraceEnabled() && !val.getStatusCode().equals(StatusCode.SUCCESS)) log.trace(val.getStatusCode().name() + " storing Key : " + key , new Exception());
            }

            @Override
            public void gotData(String key, long cas) {
                rv.setCas(cas);
            }

            @Override
            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? rv.getOperation().getHandlingNode().getSocketAddress().toString() : null);
                getTimer(operationStr, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl && !client.isInWriteOnly()) ((EVCacheLatchImpl) evcacheLatch).addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public String toString() {
        return appName + "-" + client.getZone() + "-" + client.getId();
    }

    @SuppressWarnings("unchecked")
    public <T> OperationFuture<Boolean> add(String key, int exp, T o, final Transcoder<T> tc, EVCacheLatch latch) {
        Transcoder<T> t = (Transcoder<T>) ((tc == null) ? transcoder : tc);
        return asyncStore(StoreType.add, key, exp, o, t, latch);
    }

    public long incr(String key, long by, long def, int exp) {
        return mutate(Mutator.incr, key, by, def, exp);
    }


    public long decr(String key, long by, long def, int exp) {
        return mutate(Mutator.decr, key, by, def, exp);
    }

    public long mutate(final Mutator m, String key, long by, long def, int exp) {
        final String operationStr = m.name();
        final long start = System.currentTimeMillis();
        final AtomicLong rv = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch(1);
        final Operation op = opFact.mutate(m, key, by, def, exp, new OperationCallback() {
            @Override
            public void receivedStatus(OperationStatus s) {
                getTimer(operationStr, EVCacheMetricsFactory.WRITE, null, null, null).record((System.currentTimeMillis() - start), TimeUnit.MILLISECONDS);
                rv.set(new Long(s.isSuccess() ? s.getMessage() : "-1"));
            }

            @Override
            public void complete() {
                latch.countDown();
            }
        });
        mconn.enqueueOperation(key, op);
        long retVal = def;
        try {
            if(mutateOperationTimeout == null) {
                mutateOperationTimeout = EVCacheConfig.getInstance().getDynamicLongProperty(appName + ".mutate.timeout", connectionFactory.getOperationTimeout());
            }

            
            if (!latch.await(mutateOperationTimeout.get(), TimeUnit.MILLISECONDS)) {
                retVal = rv.get();
            }
        } catch (Exception e) {
            log.error("Exception on mutate operation : " + operationStr + " Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp 
                    + "; val : " + retVal + "; Elapsed Time - " + (System.currentTimeMillis() - start), e);

        }
        if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug(operationStr + " Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp 
                + "; val : " + retVal + "; Elapsed Time - " + (System.currentTimeMillis() - start));
        return retVal;
    }

    public void reconnectNode(EVCacheNodeImpl evcNode ) {
        final long upTime = System.currentTimeMillis() - evcNode.getCreateTime();
        if (log.isDebugEnabled()) log.debug("Reconnecting node : " + evcNode + "; UpTime : " + upTime);
        if(upTime > 30000) { //not more than once every 30 seconds : TODO make this configurable
            final List<Tag> tagList = new ArrayList<Tag>(5);
            tagList.addAll(client.getTagList());
            tagList.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.RECONNECT));
            tagList.add(new BasicTag(EVCacheMetricsFactory.HOST, evcNode.getHostName()));
            EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.CONFIG, tagList);
            evcNode.setConnectTime(System.currentTimeMillis());
            mconn.queueReconnect(evcNode);
        }
    }

}
