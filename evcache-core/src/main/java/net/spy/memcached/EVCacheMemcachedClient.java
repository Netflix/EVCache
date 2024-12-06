package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.function.BiPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheAsciiOperationFactory;
import com.netflix.evcache.operation.EVCacheBulkGetFuture;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.ipc.IpcStatus;

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
import net.spy.memcached.ops.StatsOperation;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;
import net.spy.memcached.protocol.ascii.ExecCmdOperation;
import net.spy.memcached.protocol.ascii.MetaDebugOperation;
import net.spy.memcached.protocol.ascii.MetaGetOperation;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS",
"SIC_INNER_SHOULD_BE_STATIC_ANON" })
public class EVCacheMemcachedClient extends MemcachedClient {

    private static final Logger log = LoggerFactory.getLogger(EVCacheMemcachedClient.class);
    private final String appName;
    private final Property<Integer> readTimeout;

    private final EVCacheClient client;
    private final Map<String, Timer> timerMap = new ConcurrentHashMap<String, Timer>();
    private final Map<String, DistributionSummary> distributionSummaryMap = new ConcurrentHashMap<String, DistributionSummary>();

    private Property<Long> mutateOperationTimeout;
    private final ConnectionFactory connectionFactory;
    private final Property<Integer> maxReadDuration, maxWriteDuration;
    private final Property<Boolean> enableDebugLogsOnWrongKey;

    public EVCacheMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs,
                                  Property<Integer> readTimeout, EVCacheClient client) throws IOException {
        super(cf, addrs);
        this.connectionFactory = cf;
        this.readTimeout = readTimeout;
        this.client = client;
        this.appName = client.getAppName();
        this.maxWriteDuration = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".max.write.duration.metric", Integer.class).orElseGet("evcache.max.write.duration.metric").orElse(50);
        this.maxReadDuration = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".max.read.duration.metric", Integer.class).orElseGet("evcache.max.read.duration.metric").orElse(20);
        this.enableDebugLogsOnWrongKey = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".enable.debug.logs.on.wrongkey", Boolean.class).orElse(false);
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

    // Returns 'true' if keys don't match and logs & reports the error.
    // Returns 'false' if keys match.
    // TODO: Consider removing this code once we've fixed the Wrong key bug(s)
    private boolean isWrongKeyReturned(String original_key, String returned_key) {
        if (!original_key.equals(returned_key)) {
            // If they keys don't match, log the error along with the key owning host's information and stack trace.
            final String original_host = getHostNameByKey(original_key);
            final String returned_host = getHostNameByKey(returned_key);
            log.error("Wrong key returned. Key - " + original_key + " (Host: " + original_host + ") ; Returned Key "
                        + returned_key + " (Host: " + returned_host + ")", new Exception());
            client.reportWrongKeyReturned(original_host);

            // If we are configured to dynamically switch log levels to DEBUG on a wrong key error, do so here.
            if (enableDebugLogsOnWrongKey.get()) {
                System.setProperty("log4j.logger.net.spy.memcached", "DEBUG");
            }
            return true;
        }
        return false;
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

                if (isWrongKeyReturned(key, k)) return;

                if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Read data : key " + key + "; flags : " + flags + "; data : " + data);
                if (data != null)  {
                    if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Key : " + key + "; val size : " + data.length);
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.GET_OPERATION, EVCacheMetricsFactory.READ, EVCacheMetricsFactory.IPC_SIZE_INBOUND).record(data.length);
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
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(EVCacheMetricsFactory.GET_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (val != null ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), host, getReadMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }
        });
        rv.setOperation(op);
        if (listener != null) rv.addListener(listener);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public <T> EVCacheBulkGetFuture<T> asyncGetBulk(Collection<String> keys,
                                                    final Transcoder<T> tc,
                                                    EVCacheGetOperationListener<T> listener) {
        return asyncGetBulk(keys, tc, listener, (node, key) -> true);
    }

    public <T> EVCacheBulkGetFuture<T> asyncGetBulk(Collection<String> keys,
                                                    final Transcoder<T> tc,
                                                    EVCacheGetOperationListener<T> listener,
                                                    BiPredicate<MemcachedNode, String> nodeValidator) {
        final Map<String, Future<T>> m = new ConcurrentHashMap<String, Future<T>>();

        // Break the gets down into groups by key
        final Map<MemcachedNode, Collection<String>> chunks = new HashMap<MemcachedNode, Collection<String>>();
        final NodeLocator locator = mconn.getLocator();

        //Populate Node and key Map
        for (String key : keys) {
            StringUtils.validateKey(key, opFact instanceof BinaryOperationFactory);
            final MemcachedNode primaryNode = locator.getPrimary(key);
            if (primaryNode.isActive() && nodeValidator.test(primaryNode, key)) {
                Collection<String> ks = chunks.computeIfAbsent(primaryNode, k -> new ArrayList<>());
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
            public void receivedStatus(OperationStatus status) {
                if (log.isDebugEnabled()) log.debug("GetBulk Keys : " + keys + "; Status : " + status.getStatusCode().name() + "; Message : " + status.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                rv.setStatus(status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
                if (data != null)  {
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.BULK_OPERATION, EVCacheMetricsFactory.READ, EVCacheMetricsFactory.IPC_SIZE_INBOUND).record(data.length);
                }
                m.put(k, tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize())));
            }

            @Override
            public void complete() {
                if (pendingChunks.decrementAndGet() <= 0) {
                    latch.countDown();
                    getTimer(EVCacheMetricsFactory.BULK_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (m.size() == keys.size() ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), null, getReadMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(EVCacheMetricsFactory.GET_AND_TOUCH_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (val != null ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), host, getReadMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }

            public void gotData(String k, int flags, long cas, byte[] data) {
                if (isWrongKeyReturned(key, k)) return;

                if (data != null) getDataSizeDistributionSummary(EVCacheMetricsFactory.GET_AND_TOUCH_OPERATION, EVCacheMetricsFactory.READ, EVCacheMetricsFactory.IPC_SIZE_INBOUND).record(data.length);
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
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(EVCacheMetricsFactory.DELETE_OPERATION, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(EVCacheMetricsFactory.TOUCH_OPERATION, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
        if(co != null && co.getData() != null) getDataSizeDistributionSummary(EVCacheMetricsFactory.AOA_OPERATION, EVCacheMetricsFactory.WRITE, EVCacheMetricsFactory.IPC_SIZE_OUTBOUND).record(co.getData().length);
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
                    final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                    getTimer(EVCacheMetricsFactory.AOA_OPERATION_APPEND, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);;
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
                                        final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                                        getTimer(EVCacheMetricsFactory.AOA_OPERATION_REAPPEND, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
                                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                                getTimer(EVCacheMetricsFactory.AOA_OPERATION_ADD, EVCacheMetricsFactory.WRITE, rv.getStatus(), EVCacheMetricsFactory.YES, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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

    private Timer getTimer(String operation, String operationType, OperationStatus status, String hit, String host, long maxDuration) {
        String name = ((status != null) ? operation + status.getMessage() : operation );
        if(hit != null) name = name + hit;

        Timer timer = timerMap.get(name);
        if(timer != null) return timer;

        final List<Tag> tagList = new ArrayList<Tag>(client.getTagList().size() + 4 + (host == null ? 0 : 1));
        tagList.addAll(client.getTagList());
        if(operation != null) tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TAG, operation));
        if(operationType != null) tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TYPE_TAG, operationType));
        if(status != null) {
            if(status.getStatusCode() == StatusCode.SUCCESS || status.getStatusCode() == StatusCode.ERR_NOT_FOUND || status.getStatusCode() == StatusCode.ERR_EXISTS) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.IPC_RESULT, EVCacheMetricsFactory.SUCCESS));
            } else {
                tagList.add(new BasicTag(EVCacheMetricsFactory.IPC_RESULT, EVCacheMetricsFactory.FAIL));
            }
            tagList.add(new BasicTag(EVCacheMetricsFactory.IPC_STATUS, getStatusCode(status.getStatusCode())));
        }
        if(hit != null) tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE_HIT, hit));
        if(host != null) tagList.add(new BasicTag(EVCacheMetricsFactory.FAILED_HOST, host));

        timer = EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.IPC_CALL, tagList, Duration.ofMillis(maxDuration));
        timerMap.put(name, timer);
        return timer;
    }
    
    private String getStatusCode(StatusCode sc) {
        return EVCacheMetricsFactory.getInstance().getStatusCode(sc);
    }

    private DistributionSummary getDataSizeDistributionSummary(String operation, String type, String metric) {
        DistributionSummary distributionSummary = distributionSummaryMap.get(operation);
        if(distributionSummary != null) return distributionSummary;

        final List<Tag> tagList = new ArrayList<Tag>(6);
        tagList.addAll(client.getTagList());
        tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TAG, operation));
        tagList.add(new BasicTag(EVCacheMetricsFactory.CALL_TYPE_TAG, type));
        distributionSummary = EVCacheMetricsFactory.getInstance().getDistributionSummary(metric, tagList);
        distributionSummaryMap.put(operation, distributionSummary);
        return distributionSummary;
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
        if (storeType == StoreType.set) {
            operationStr = EVCacheMetricsFactory.SET_OPERATION;
        } else if (storeType == StoreType.add) {
            operationStr = EVCacheMetricsFactory.ADD_OPERATION;
        } else {
            operationStr = EVCacheMetricsFactory.REPLACE_OPERATION;
        }
        if(co != null && co.getData() != null) getDataSizeDistributionSummary(operationStr, EVCacheMetricsFactory.WRITE, EVCacheMetricsFactory.IPC_SIZE_OUTBOUND).record(co.getData().length);

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
                final String host = (((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) || rv.getStatus().getStatusCode().equals(StatusCode.ERR_NO_MEM)) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(operationStr, EVCacheMetricsFactory.WRITE, rv.getStatus(), null, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
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
        final List<OperationStatus> statusList = new ArrayList<OperationStatus>(1);
        final Operation op = opFact.mutate(m, key, by, def, exp, new OperationCallback() {
            @Override
            public void receivedStatus(OperationStatus s) {
                statusList.add(s);
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
                mutateOperationTimeout = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".mutate.timeout", Long.class).orElse(connectionFactory.getOperationTimeout());
            }


            if (!latch.await(mutateOperationTimeout.get(), TimeUnit.MILLISECONDS)) {
                if (log.isDebugEnabled()) log.debug("Mutation operation timeout. Will return -1");
                retVal = -1;
            } else {
                retVal = rv.get();
            }

        } catch (Exception e) {
            log.error("Exception on mutate operation : " + operationStr + " Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp
                    + "; val : " + retVal + "; Elapsed Time - " + (System.currentTimeMillis() - start), e);

        }
        final OperationStatus status = statusList.size() > 0 ? statusList.get(0) : null;
        final String host = ((status != null && status.getStatusCode().equals(StatusCode.TIMEDOUT) && op != null) ? getHostName(op.getHandlingNode().getSocketAddress()) : null);
        getTimer(operationStr, EVCacheMetricsFactory.WRITE, status, null, host, getWriteMetricMaxValue()).record((System.currentTimeMillis() - start), TimeUnit.MILLISECONDS);
        if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug(operationStr + " Key : " + key + "; by : " + by + "; default : " + def + "; exp : " + exp
                + "; val : " + retVal + "; Elapsed Time - " + (System.currentTimeMillis() - start));
        return retVal;
    }

    public void reconnectNode(EVCacheNode evcNode ) {
        final long upTime = System.currentTimeMillis() - evcNode.getCreateTime();
        if (log.isDebugEnabled()) log.debug("Reconnecting node : " + evcNode + "; UpTime : " + upTime);
        if(upTime > 30000) { //not more than once every 30 seconds : TODO make this configurable
            final List<Tag> tagList = new ArrayList<Tag>(client.getTagList().size() + 2);
            tagList.addAll(client.getTagList());
            tagList.add(new BasicTag(EVCacheMetricsFactory.CONFIG_NAME, EVCacheMetricsFactory.RECONNECT));
            tagList.add(new BasicTag(EVCacheMetricsFactory.FAILED_HOST, evcNode.getHostName()));
            EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.CONFIG, tagList);
            evcNode.setConnectTime(System.currentTimeMillis());
            mconn.queueReconnect(evcNode);
        }
    }

    public int getWriteMetricMaxValue() {
        return maxWriteDuration.get().intValue();
    }

    public int getReadMetricMaxValue() {
        return maxReadDuration.get().intValue();
    }

    private String getHostNameByKey(String key) {
        MemcachedNode evcNode = getEVCacheNode(key);
        return getHostName(evcNode.getSocketAddress());
    }

    private String getHostName(SocketAddress sa) {
        if (sa == null) return null;
        if(sa instanceof InetSocketAddress) {
            return ((InetSocketAddress)sa).getHostName();
        } else {
            return sa.toString();
        }
    }

    public EVCacheItemMetaData metaDebug(String key) {
        final CountDownLatch latch = new CountDownLatch(1);
        final EVCacheItemMetaData rv = new EVCacheItemMetaData();
        if(opFact instanceof EVCacheAsciiOperationFactory) {
        final Operation op = ((EVCacheAsciiOperationFactory)opFact).metaDebug(key, new MetaDebugOperation.Callback() {
            public void receivedStatus(OperationStatus status) {
                if (!status.isSuccess()) {
                    if (log.isDebugEnabled()) log.debug("Unsuccessful stat fetch: %s", status);
                  }
                if (log.isDebugEnabled()) log.debug("Getting Meta Debug: " + key + "; Status : " + status.getStatusCode().name() + (log.isTraceEnabled() ?  " Node : " + getEVCacheNode(key) : "") + "; Message : " + status.getMessage());
            }

            public void debugInfo(String k, String val) {
                if (log.isDebugEnabled()) log.debug("key " + k + "; val : " + val);
                if(k.equals("exp")) rv.setSecondsLeftToExpire(Long.parseLong(val) * -1);
                else if(k.equals("la")) rv.setSecondsSinceLastAccess(Long.parseLong(val));
                else if(k.equals("cas")) rv.setCas(Long.parseLong(val));
                else if(k.equals("fetch")) rv.setHasBeenFetchedAfterWrite(Boolean.parseBoolean(val));
                else if(k.equals("cls")) rv.setSlabClass(Integer.parseInt(val));
                else if(k.equals("size")) rv.setSizeInBytes(Integer.parseInt(val));
            }

            public void complete() {
                latch.countDown();
            }});
            mconn.enqueueOperation(key, op);
            try {
                if (!latch.await(operationTimeout, TimeUnit.MILLISECONDS)) {
                    if (log.isDebugEnabled()) log.debug("meta debug operation timeout. Will return empty opbject.");
                }
            } catch (Exception e) {
                log.error("Exception on meta debug operation : Key : " + key, e);
            }
            if (log.isDebugEnabled()) log.debug("Meta Debug Data : " + rv);
        }
        return rv;
    }

    public Map<SocketAddress, String> execCmd(final String cmd, String[] ips) {
        final Map<SocketAddress, String> rv = new HashMap<SocketAddress, String>();
        Collection<MemcachedNode> nodes = null;
        if(ips == null || ips.length == 0) {
            nodes = mconn.getLocator().getAll();
        } else {
            nodes = new ArrayList<MemcachedNode>(ips.length);
            for(String ip : ips) {
                for(MemcachedNode node : mconn.getLocator().getAll()) {
                    if(((InetSocketAddress)node.getSocketAddress()).getAddress().getHostAddress().equals(ip)) {
                        nodes.add(node);
                    }
                }
            }
        }
        if(nodes != null && !nodes.isEmpty()) {
            CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
                @Override
                public Operation newOp(final MemcachedNode n, final CountDownLatch latch) {
                    final SocketAddress sa = n.getSocketAddress();
                    return ((EVCacheAsciiOperationFactory)opFact).execCmd(cmd, new ExecCmdOperation.Callback() {
    
                        @Override
                        public void receivedStatus(OperationStatus status) {
                            if (log.isDebugEnabled()) log.debug("cmd : " + cmd + "; MemcachedNode : " + n + "; Status : " + status);
                            rv.put(sa, status.getMessage());
                        }
    
                        @Override
                        public void complete() {
                            latch.countDown();
                        }
                    });
                }
            }, nodes);
            try {
                blatch.await(operationTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted waiting for stats", e);
            }
        }
        return rv;
    }

    public <T> EVCacheOperationFuture<EVCacheItem<T>> asyncMetaGet(final String key, final Transcoder<T> tc, EVCacheGetOperationListener<T> listener) {
        final CountDownLatch latch = new CountDownLatch(1);

        final EVCacheOperationFuture<EVCacheItem<T>> rv = new EVCacheOperationFuture<EVCacheItem<T>>(key, latch, new AtomicReference<EVCacheItem<T>>(null), readTimeout.get().intValue(), executorService, client);
        if(opFact instanceof EVCacheAsciiOperationFactory) {
        final Operation op = ((EVCacheAsciiOperationFactory)opFact).metaGet(key, new MetaGetOperation.Callback() {

            private EVCacheItem<T> evItem = new EVCacheItem<T>();

            public void receivedStatus(OperationStatus status) {
                if (log.isDebugEnabled()) log.debug("Getting Key : " + key + "; Status : " + status.getStatusCode().name() + (log.isTraceEnabled() ?  " Node : " + getEVCacheNode(key) : "")
                        + "; Message : " + status.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - rv.getStartTime()));
                try {
                    if (evItem.getData() != null) {
                        if (log.isTraceEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.trace("Key : " + key + "; val : " + evItem);
                        rv.set(evItem, status);
                    } else {
                        if (log.isTraceEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.trace("Key : " + key + "; val is null");
                        rv.set(null, status);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    rv.set(null, status);
                }
            }

            @Override
            public void gotMetaData(String k, char flag, String fVal) {
                if (log.isDebugEnabled()) log.debug("key " + k + "; val : " + fVal + "; flag : " + flag);
                if (isWrongKeyReturned(key, k)) return;

                switch (flag) {
                case 's':
                    evItem.getItemMetaData().setSizeInBytes(Integer.parseInt(fVal));
                    break;

                case 'c':
                    evItem.getItemMetaData().setCas(Long.parseLong(fVal));
                    break;

                case 'f':
                    evItem.setFlag(Integer.parseInt(fVal));
                    break;

                case 'h':
                    evItem.getItemMetaData().setHasBeenFetchedAfterWrite(fVal.equals("1"));
                    break;

                case 'l':
                    evItem.getItemMetaData().setSecondsSinceLastAccess(Long.parseLong(fVal));
                    break;

                case 'O':
                    //opaque = val;
                    break;

                case 't':
                    final int ttlLeft = Integer.parseInt(fVal);
                    evItem.getItemMetaData().setSecondsLeftToExpire(ttlLeft);
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.META_GET_OPERATION, EVCacheMetricsFactory.READ, EVCacheMetricsFactory.INTERNAL_TTL).record(ttlLeft);
                    break;

                default:
                    break;
                }
            }

            @Override
            public void gotData(String k, int flag, byte[] data) {
                if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Read data : key " + k + "; flags : " + flag + "; data : " + data);
                if (isWrongKeyReturned(key, k)) return;

                if (data != null)  {
                    if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Key : " + k + "; val size : " + data.length);
                    getDataSizeDistributionSummary(EVCacheMetricsFactory.META_GET_OPERATION, EVCacheMetricsFactory.READ, EVCacheMetricsFactory.IPC_SIZE_INBOUND).record(data.length);
                    if (tc == null) {
                        if (tcService == null) {
                            log.error("tcService is null, will not be able to decode");
                            throw new RuntimeException("TranscoderSevice is null. Not able to decode");
                        } else {
                            final Transcoder<T> t = (Transcoder<T>) getTranscoder();
                            final T item = t.decode(new CachedData(flag, data, t.getMaxSize()));
                            evItem.setData(item);
                        }
                    } else {
                        if (tcService == null) {
                            log.error("tcService is null, will not be able to decode");
                            throw new RuntimeException("TranscoderSevice is null. Not able to decode");
                        } else {
                            final T item = tc.decode(new CachedData(flag, data, tc.getMaxSize()));
                            evItem.setData(item);
                        }
                    }
                } else {
                    if (log.isDebugEnabled() && client.getPool().getEVCacheClientPoolManager().shouldLog(appName)) log.debug("Key : " + k + "; val is null" );
                }
            }

            public void complete() {
                latch.countDown();
                final String host = ((rv.getStatus().getStatusCode().equals(StatusCode.TIMEDOUT) && rv.getOperation() != null) ? getHostName(rv.getOperation().getHandlingNode().getSocketAddress()) : null);
                getTimer(EVCacheMetricsFactory.META_GET_OPERATION, EVCacheMetricsFactory.READ, rv.getStatus(), (evItem.getData() != null ? EVCacheMetricsFactory.YES : EVCacheMetricsFactory.NO), host, getReadMetricMaxValue()).record((System.currentTimeMillis() - rv.getStartTime()), TimeUnit.MILLISECONDS);
                rv.signalComplete();
            }

            });
            rv.setOperation(op);
            mconn.enqueueOperation(key, op);
            if (log.isDebugEnabled()) log.debug("Meta_Get Data : " + rv);
        }
        return rv;
    }
}
