package com.netflix.evcache.pool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;

import net.spy.memcached.CachedData;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;

public class EVCacheClientUtil {
    private static Logger log = LoggerFactory.getLogger(EVCacheClientUtil.class);
    private final ChunkTranscoder ct = new ChunkTranscoder();
    private final String _appName;
    private final DynamicBooleanProperty fixup;
    private final DynamicIntProperty fixupPoolSize;
    private final EVCacheClientPool _pool;
    private ThreadPoolExecutor threadPool = null;

    public EVCacheClientUtil(EVCacheClientPool pool) {
        this._pool = pool;
        this._appName = pool.getAppName();
        this.fixup = EVCacheConfig.getInstance().getDynamicBooleanProperty(_appName + ".addOperation.fixup", Boolean.FALSE);
        this.fixupPoolSize = EVCacheConfig.getInstance().getDynamicIntProperty(_appName + ".addOperation.fixup.poolsize", 10);

        RejectedExecutionHandler block = new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                final ArrayList<Tag> tags = new ArrayList<Tag>(2);
                tags.add(new BasicTag(EVCacheMetricsFactory.CACHE, _appName));
                tags.add(new BasicTag(EVCacheMetricsFactory.STATUS, "rejected"));
                EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.INTERNAL_ADD_CALL_FIXUP, tags);
            }
        };
        
        class SimpleThreadFactory implements ThreadFactory {
            private final AtomicInteger counter = new AtomicInteger(); 
            public Thread newThread(Runnable r) {
                final Thread t = new Thread(r, "EVCacheClientUtil-AddFixUp-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
          }

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(10000);
        threadPool = new ThreadPoolExecutor(fixupPoolSize.get(), fixupPoolSize.get() * 2, 30, TimeUnit.SECONDS, queue, new SimpleThreadFactory(), block);
        threadPool.prestartAllCoreThreads();
    }

    public EVCacheLatch add(String canonicalKey, CachedData cd, int timeToLive, Policy policy) throws Exception {
        if (cd == null) return null; 
        
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName){

            @Override
            public void onComplete(OperationFuture<?> operationFuture) throws Exception {
                super.onComplete(operationFuture);
                if (getPendingFutureCount() == 0 && fixup.get()) {
                    final RemoteRequest req = new RemoteRequest(this, canonicalKey, timeToLive);
                    threadPool.submit(req);
                }
            }
        };

        for (EVCacheClient client : clients) {
            final Future<Boolean> future = client.add(canonicalKey, timeToLive, cd, latch);
            if(log.isDebugEnabled()) log.debug("ADD : Op Submitted : APP " + _appName + ", key " + canonicalKey + "; future : " + future);
        }
        return latch;
    }

    class RemoteRequest implements Runnable {
        private EVCacheLatchImpl latch;
        private String canonicalKey;
        private int timeToLive;
        public RemoteRequest(EVCacheLatchImpl latch, String canonicalKey, int timeToLive) {
            this.latch = latch;
            this.canonicalKey = canonicalKey;
            this.timeToLive = timeToLive;
        }
        public void run() {
            final List<Future<Boolean>> futures = latch.getAllFutures();
            int successCount = 0, failCount = 0;
            for(int i = 0; i < futures.size() ; i++) {
                final Future<Boolean> future = futures.get(i);
                if(future instanceof EVCacheOperationFuture) {
                    final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)future;
                    if(f.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                        successCount++;
                        if(log.isDebugEnabled()) log.debug("ADD : Success : APP " + _appName + ", key " + canonicalKey+ ", ServerGroup : " + f.getServerGroup().getName());
                    } else {
                        failCount++;
                        if(log.isDebugEnabled()) log.debug("ADD : Fail : APP " + _appName + ", key : " + canonicalKey + ", ServerGroup : " + f.getServerGroup().getName());
                    }
                }
            }
            if(log.isDebugEnabled()) log.debug("ADD : Status: APP " + _appName + ", key : " + canonicalKey + ", failCount : " + failCount + "; successCount : " + successCount);

            if(successCount > 0 && failCount > 0) {
                CachedData readData = null;
                for(int i = 0; i < futures.size(); i++) {
                    final Future<Boolean> evFuture = futures.get(i);
                    if(evFuture instanceof EVCacheOperationFuture) {
                        final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)evFuture;
                        if(f.getStatus().getStatusCode() == StatusCode.ERR_EXISTS) {
                            final EVCacheClient client = _pool.getEVCacheClient(f.getServerGroup());
                            if(client != null) {
                                try {
                                    readData = client.get(canonicalKey, ct, false, false);
                                } catch (Exception e) {
                                    log.error("Error reading the data", e);
                                }
                                if(log.isDebugEnabled()) log.debug("Add : Read existing data for: APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName());
                                if(readData != null) {
                                    break;
                                } else {
                                    
                                }
                            }
                        }
                    }
                }
                if(readData != null) {
                    for(int i = 0; i < futures.size(); i++) {
                        final Future<Boolean> evFuture = futures.get(i);
                        if(evFuture instanceof OperationFuture) {
                            final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)evFuture;
                            if(f.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                                final EVCacheClient client = _pool.getEVCacheClient(f.getServerGroup());
                                if(client != null) {
                                    try {
                                        client.set(canonicalKey, readData, timeToLive, null);
                                        if(log.isDebugEnabled()) log.debug("Add: Fixup for : APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName());
                                        EVCacheMetricsFactory.getInstance().increment(EVCacheMetricsFactory.INTERNAL_ADD_CALL_FIXUP, client.getTagList());
                                    } catch (Exception e) {
                                        if(log.isDebugEnabled()) log.debug("Add: Fixup Error : APP " + _appName + ", key " + canonicalKey + "; ServerGroup : " + client.getServerGroupName(), e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
