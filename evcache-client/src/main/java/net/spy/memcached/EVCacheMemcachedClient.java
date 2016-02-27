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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.evcache.EVCacheGetOperationListener;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheBulkGetFuture;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.tag.BasicTag;
import com.netflix.servo.tag.Tag;

import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.DeleteOperation;
import net.spy.memcached.ops.GetAndTouchOperation;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.ops.StoreOperation;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
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

    public EVCacheMemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs,
            ChainedDynamicProperty.IntProperty readTimeout, String appName, String zone, int id,
            ServerGroup serverGroup) throws IOException {
        super(cf, addrs);
        this.id = id;
        this.appName = appName;
        this.zone = zone;
        this.readTimeout = readTimeout;
        this.serverGroup = serverGroup;
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
        final EVCacheOperationFuture<T> rv = new EVCacheOperationFuture<T>(key, latch, new AtomicReference<T>(null), readTimeout.get().intValue(), executorService, appName, serverGroup, "GetOperation");
        Operation op = opFact.get(key, new GetOperation.Callback() {
            private Future<T> val = null;

            public void receivedStatus(OperationStatus status) {
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
                if (!key.equals(k)) log.warn("Wrong key returned. Key - " + key + "; Returned Key " + k);
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

        GetOperation.Callback cb = new GetOperation.Callback() {
            @Override
            @SuppressWarnings("synthetic-access")
            public void receivedStatus(OperationStatus status) {
                rv.setStatus(status);
            }

            @Override
            public void gotData(String k, int flags, byte[] data) {
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
        final EVCacheOperationFuture<CASValue<T>> rv = new EVCacheOperationFuture<CASValue<T>>(key, latch, new AtomicReference<CASValue<T>>(null), operationTimeout, executorService, appName, serverGroup, "GetOperation");
        Operation op = opFact.getAndTouch(key, exp, new GetAndTouchOperation.Callback() {
            private CASValue<T> val = null;

            public void receivedStatus(OperationStatus status) {
                rv.set(val, status);
            }

            public void complete() {
                latch.countDown();
                rv.signalComplete();
            }

            public void gotData(String k, int flags, long cas, byte[] data) {
                if (!key.equals(k)) log.warn("Wrong key returned. Key - " + key + "; Returned Key " + k);
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
        final OperationFuture<Boolean> rv = new OperationFuture<Boolean>(key, latch, operationTimeout, executorService);

        final DeleteOperation.Callback callback = new DeleteOperation.Callback() {
            @Override
            public void receivedStatus(OperationStatus s) {
                rv.set(Boolean.TRUE, s);
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
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl) ((EVCacheLatchImpl) evcacheLatch)
                .addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    private <T> OperationFuture<Boolean> asyncStore(final StoreType storeType, final String key, int exp, T value,
            Transcoder<T> tc, EVCacheLatch evcacheLatch) {
        CachedData co;
        if (value instanceof CachedData) {
            co = (CachedData) value;
        } else {
            co = tc.encode(value);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final String operationStr;
        if (storeType == StoreType.set) {
            operationStr = "Set";
        } else if (storeType == StoreType.add) {
            operationStr = "Add";
        } else {
            operationStr = "Replace";
        }
        final OperationFuture<Boolean> rv = new EVCacheOperationFuture<Boolean>(key, latch, new AtomicReference<Boolean>(null), operationTimeout, executorService, appName, serverGroup, "Latency" + operationStr);
        Operation op = opFact.store(storeType, key, co.getFlags(), exp, co.getData(), new StoreOperation.Callback() {
            final Stopwatch operationDuration = EVCacheMetricsFactory.getStatsTimer(appName, serverGroup, "Latency" + operationStr).start();

            @Override
            public void receivedStatus(OperationStatus val) {
                operationDuration.stop();
                if (log.isDebugEnabled()) log.debug("Storing Key : " + key + "; Status : " + val.getStatusCode().name()
                        + "; Message : " + val.getMessage() + "; Elapsed Time - " + (System.currentTimeMillis() - operationDuration.getDuration()));

                Tag tag = null;
                final MemcachedNode node = getEVCacheNode(key);
                if (node.getSocketAddress() instanceof InetSocketAddress) {
                    tag = new BasicTag("HOST", ((InetSocketAddress) node.getSocketAddress()).getHostName());
                }

                if (val.getStatusCode().equals(StatusCode.SUCCESS)) {
                    EVCacheMetricsFactory.getCounter(appName + "-" + serverGroup.getName() + "-" + operationStr
                            + "Call-SUCCESS").increment();
                } else if (val.getStatusCode().equals(StatusCode.TIMEDOUT)) {
                    EVCacheMetricsFactory.getCounter(appName + "-" + serverGroup.getName() + "-" + operationStr
                            + "Call-TIMEDOUT", tag).increment();
                } else {
                    EVCacheMetricsFactory.getCounter(appName + "-" + serverGroup.getName() + "-" + operationStr
                            + "Call-" + val.getStatusCode().name(), tag).increment();
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
        if (evcacheLatch != null && evcacheLatch instanceof EVCacheLatchImpl) ((EVCacheLatchImpl) evcacheLatch)
                .addFuture(rv);
        mconn.enqueueOperation(key, op);
        return rv;
    }

    public String toString() {
        return appName + "_" + zone + " _" + id;
    }

}
