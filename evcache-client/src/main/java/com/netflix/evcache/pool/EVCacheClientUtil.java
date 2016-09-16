package com.netflix.evcache.pool;

import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;

import net.spy.memcached.CachedData;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;

public class EVCacheClientUtil {
    private static Logger log = LoggerFactory.getLogger(EVCacheClientUtil.class);
    private static final ChunkTranscoder ct = new ChunkTranscoder();

    public static EVCacheLatch add(String canonicalKey, CachedData cd, int timeToLive, EVCacheClientPool _pool, Policy policy) throws Exception {
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, clients.length, _pool.getAppName()){

            @Override
            public void onComplete(OperationFuture<?> operationFuture) throws Exception {
                if(getPendingCount() > 1) {
                    super.onComplete(operationFuture);
                } else {
                    final List<Future<Boolean>> futures = getAllFutures();
                    int successCount = 0, failCount = 0;
                    for(int i = 0; i < futures.size() ; i++) {
                        final Future<Boolean> future = futures.get(i);
                        if(future instanceof EVCacheOperationFuture) {
                            final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)future;
                            if(f.getStatus().getStatusCode() == StatusCode.SUCCESS) {
                                successCount++;
                                if(log.isDebugEnabled()) log.debug("ADD Success : APP " + _pool.getAppName() + ", key " + canonicalKey);
                            } else {
                                failCount++;
                                if(log.isDebugEnabled()) log.debug("ADD Fail : APP " + _pool.getAppName() + ", key " + canonicalKey);
                            }
                        }
                    }
    
                    if(successCount > 0 && failCount > 0) {
                        CachedData readData = null;
                        for(int i = 0; i < futures.size(); i++) {
                            final Future<Boolean> evFuture = futures.get(i);
                            if(evFuture instanceof EVCacheOperationFuture) {
                                final EVCacheOperationFuture<Boolean> f = (EVCacheOperationFuture<Boolean>)evFuture;
                                if(f.getStatus().getStatusCode() == StatusCode.ERR_EXISTS) {
                                    final EVCacheClient client = _pool.getEVCacheClient(f.getServerGroup());
                                    if(client != null) {
                                        readData = client.get(canonicalKey, ct, false, false);
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
                                            futures.remove(i);
                                            client.set(canonicalKey, readData, timeToLive, this);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    super.onComplete(operationFuture);
                }
            }
        };

        for (EVCacheClient client : clients) {
            final Future<Boolean> future = client.add(canonicalKey, timeToLive, cd, ct, latch);
            if(log.isDebugEnabled()) log.debug("ADD Op Submitted : APP " + _pool.getAppName() + ", key " + canonicalKey + "; future : " + future);
        }
        return latch;
    }


}
