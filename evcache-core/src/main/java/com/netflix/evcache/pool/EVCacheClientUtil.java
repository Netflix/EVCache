package com.netflix.evcache.pool;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netflix.evcache.EVCacheKey;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.operation.EVCacheLatchImpl;

import net.spy.memcached.CachedData;

public class EVCacheClientUtil {
    private static final Logger log = LoggerFactory.getLogger(EVCacheClientUtil.class);
    private final ChunkTranscoder ct = new ChunkTranscoder();
    private final String _appName;
    private final EVCacheClientPool _pool;

    public EVCacheClientUtil(EVCacheClientPool pool) {
        this._pool = pool;
        this._appName = pool.getAppName();
    }

    /**
     * TODO : once metaget is available we need to get the remaining ttl from an existing entry and use it 
     */
    public EVCacheLatch add(EVCacheKey evcKey, final CachedData cd, boolean shouldHashKey, Transcoder evcacheValueTranscoder, int timeToLive, Policy policy) throws Exception {
        if (cd == null) return null; 
        
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);

        CachedData cd1 = null;
        Boolean firstStatus = null;
        for (EVCacheClient client : clients) {
            String key = evcKey.getDerivedKey(client.isDuetClient());
            if (shouldHashKey) {
            	if(cd1 == null) {
	                final EVCacheValue val = new EVCacheValue(evcKey.getCanonicalKey(client.isDuetClient()), cd.getData(), cd.getFlags(), timeToLive, System.currentTimeMillis());
	                cd1 = evcacheValueTranscoder.encode(val);
            	}
            } else {
            	cd1 = cd;
            }
            final Future<Boolean> f = client.add(key, timeToLive, cd1, latch);
            if (log.isDebugEnabled()) log.debug("ADD : Op Submitted : APP " + _appName + ", key " + key + "; future : " + f + "; client : " + client);
            boolean status = f.get().booleanValue();
            if(!status) { // most common case
                if(firstStatus == null) {
                    for(int i = 0; i < clients.length; i++) {
                        latch.countDown();
                    }
                    return latch;
                } else {
                    return fixup(client, clients, evcKey, timeToLive, policy);
                }
            }
            if(firstStatus == null) firstStatus = Boolean.valueOf(status);
        }
        return latch;
    }

    private EVCacheLatch fixup(EVCacheClient sourceClient, EVCacheClient[] destClients, EVCacheKey evcKey, int timeToLive, Policy policy) {
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, destClients.length, _appName);
        try {
            final CachedData readData = sourceClient.get(evcKey.getDerivedKey(sourceClient.isDuetClient()), ct, false, false);

            if(readData != null) {
                sourceClient.touch(evcKey.getDerivedKey(sourceClient.isDuetClient()), timeToLive);
                for(EVCacheClient destClient : destClients) {
                    destClient.set(evcKey.getDerivedKey(destClient.isDuetClient()), readData, timeToLive, latch);
                }
            }
            latch.await(_pool.getOperationTimeout().get(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error reading the data", e);
        }
        return latch;
    }
}