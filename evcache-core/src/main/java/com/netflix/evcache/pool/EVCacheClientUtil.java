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
    private final long _operationTimeout;

    public EVCacheClientUtil(String appName, long operationTimeout) {
        this._appName = appName;
        this._operationTimeout = operationTimeout;
    }

    //TODO: Remove this todo. This method has been made hashing agnostic.
    /**
     * TODO : once metaget is available we need to get the remaining ttl from an existing entry and use it
     */
    public EVCacheLatch add(EVCacheKey evcKey, final CachedData cd, Transcoder evcacheValueTranscoder, int timeToLive, Policy policy, final EVCacheClient[] clients, int latchCount, boolean fixMissing) throws Exception {
        if (cd == null) return null;

        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, latchCount, _appName);

        CachedData cdHashed = null;
        Boolean firstStatus = null;
        for (EVCacheClient client : clients) {
            CachedData cd1;
            if (evcKey.getHashKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxDigestBytes(), client.getMaxHashBytes()) != null) {
                if(cdHashed == null) {
                    final EVCacheValue val = new EVCacheValue(evcKey.getCanonicalKey(client.isDuetClient()), cd.getData(), cd.getFlags(), timeToLive, System.currentTimeMillis());
                    cdHashed = evcacheValueTranscoder.encode(val);
                }
                cd1 = cdHashed;
            } else {
            	cd1 = cd;
            }
            String key = evcKey.getDerivedKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxDigestBytes(), client.getMaxHashBytes());
            final Future<Boolean> f = client.add(key, timeToLive, cd1, latch);
            if (log.isDebugEnabled()) log.debug("ADD : Op Submitted : APP " + _appName + ", key " + key + "; future : " + f + "; client : " + client);
            if(fixMissing) {
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
        }
        return latch;
    }

    private EVCacheLatch fixup(EVCacheClient sourceClient, EVCacheClient[] destClients, EVCacheKey evcKey, int timeToLive, Policy policy) {
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, destClients.length, _appName);
        try {
            final CachedData readData = sourceClient.get(evcKey.getDerivedKey(sourceClient.isDuetClient(), sourceClient.getHashingAlgorithm(), sourceClient.shouldEncodeHashKey(), sourceClient.getMaxDigestBytes(), sourceClient.getMaxHashBytes()), ct, false, false);

            if(readData != null) {
                sourceClient.touch(evcKey.getDerivedKey(sourceClient.isDuetClient(), sourceClient.getHashingAlgorithm(), sourceClient.shouldEncodeHashKey(), sourceClient.getMaxDigestBytes(), sourceClient.getMaxHashBytes()), timeToLive);
                for(EVCacheClient destClient : destClients) {
                    destClient.set(evcKey.getDerivedKey(destClient.isDuetClient(), destClient.getHashingAlgorithm(), destClient.shouldEncodeHashKey(), destClient.getMaxDigestBytes(), destClient.getMaxHashBytes()), readData, timeToLive, latch);
                }
            }
            latch.await(_operationTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error reading the data", e);
        }
        return latch;
    }
}