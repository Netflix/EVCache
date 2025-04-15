package com.netflix.evcache.pool;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netflix.evcache.EVCacheKey;
import com.netflix.evcache.operation.EVCacheItem;
import net.spy.memcached.MemcachedClientIF;
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

    /**
     * Maximum supported key length.
     */
    private static final int MAX_KEY_BYTES = MemcachedClientIF.MAX_KEY_LENGTH;

    /**
     * Exception thrown if the input key is too long.
     */
    private static final IllegalArgumentException KEY_TOO_LONG_EXCEPTION =
            new IllegalArgumentException("Key is too long (maxlen = "
                    + MAX_KEY_BYTES + ')');

    /**
     * Exception thrown if the input key is empty.
     */
    private static final IllegalArgumentException KEY_EMPTY_EXCEPTION =
            new IllegalArgumentException("Key must contain at least one character.");

    public EVCacheClientUtil(String appName, long operationTimeout) {
        this._appName = appName;
        this._operationTimeout = operationTimeout;
    }

    //TODO: Remove this todo. This method has been made hashing agnostic.
    /**
     * TODO : once metaget is available we need to get the remaining ttl from an existing entry and use it
     */
    public EVCacheLatch add(EVCacheKey evcKey, final CachedData cd, Transcoder evcacheValueTranscoder, int timeToLive, Policy policy, final EVCacheClient[] clients, int latchCount, boolean fixMissing, boolean bypassAddOpt, boolean fixupAsFail, boolean newFixup) throws Exception {
        if (cd == null) return null;

        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, latchCount, _appName);

        CachedData cdHashed = null;
        Boolean firstStatus = null;
        for (EVCacheClient client : clients) {
            CachedData cd1;
            if (evcKey.getHashKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxDigestBytes(), client.getMaxHashLength(), client.getBaseEncoder()) != null) {
                if(cdHashed == null) {
                    final EVCacheValue val = new EVCacheValue(evcKey.getCanonicalKey(client.isDuetClient()), cd.getData(), cd.getFlags(), timeToLive, System.currentTimeMillis());
                    cdHashed = evcacheValueTranscoder.encode(val);
                }
                cd1 = cdHashed;
            } else {
            	cd1 = cd;
            }
            String key = evcKey.getDerivedKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxDigestBytes(), client.getMaxHashLength(), client.getBaseEncoder());
            final Future<Boolean> f = client.add(key, timeToLive, cd1, latch);
            if (log.isDebugEnabled()) log.debug("ADD : Op Submitted : APP " + _appName + ", key " + key + "; future : " + f + "; client : " + client);
            if(fixMissing) {
                boolean status = f.get().booleanValue();
                if(!status) { // most common case
                    if(firstStatus == null) {
                        if (log.isDebugEnabled()) log.debug("Add failed at first client. key: " + key + ", client : " + client);
                        if(!bypassAddOpt) {
                            if (log.isDebugEnabled()) log.debug("Short circuiting due to optimization!!");
                            for (int i = 0; i < clients.length; i++) {
                                latch.countDown();
                            }
                            return latch;
                        }
                        else {
                            return fixup(client, clients, evcKey, timeToLive, policy, latch, fixupAsFail, newFixup);
                        }
                    } else {
                        if (log.isDebugEnabled()) log.debug("Add failed after first client. key: " + key + ", client : " + client);
                        return fixup(client, clients, evcKey, timeToLive, policy, latch, fixupAsFail, newFixup);
                    }
                }
                if(firstStatus == null) firstStatus = Boolean.valueOf(status);
            }
        }
        return latch;
    }

    private EVCacheLatch fixup(EVCacheClient sourceClient, EVCacheClient[] destClients, EVCacheKey evcKey, int timeToLive, Policy policy, EVCacheLatchImpl prevLatch, boolean fixupAsFail, boolean newFixup) {
        if (log.isDebugEnabled()) log.debug("Trying to fix up!! destClient count = " + destClients.length);
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy, destClients.length, _appName);
        try {
            CachedData readData = null;
            int ttlToSet = timeToLive;
            if (newFixup) {
                final EVCacheItem<CachedData> obj = sourceClient.metaGet(evcKey.getDerivedKey(sourceClient.isDuetClient(), sourceClient.getHashingAlgorithm(), sourceClient.shouldEncodeHashKey(), sourceClient.getMaxDigestBytes(), sourceClient.getMaxHashLength(), sourceClient.getBaseEncoder()), ct, false, false);
                if (obj != null) {
                    readData = obj.getData();
                    ttlToSet = (int) obj.getItemMetaData().getSecondsLeftToExpire();
                    if (log.isDebugEnabled()) log.debug("Applying new fixup. ttlToSet = " + ttlToSet);
                }
            }
            else {
                 readData = sourceClient.get(evcKey.getDerivedKey(sourceClient.isDuetClient(), sourceClient.getHashingAlgorithm(), sourceClient.shouldEncodeHashKey(), sourceClient.getMaxDigestBytes(), sourceClient.getMaxHashLength(), sourceClient.getBaseEncoder()), ct, false, false);
            }

            if(readData != null) {
                sourceClient.touch(evcKey.getDerivedKey(sourceClient.isDuetClient(), sourceClient.getHashingAlgorithm(), sourceClient.shouldEncodeHashKey(), sourceClient.getMaxDigestBytes(), sourceClient.getMaxHashLength(), sourceClient.getBaseEncoder()), ttlToSet);
                for(EVCacheClient destClient : destClients) {
                    destClient.set(evcKey.getDerivedKey(destClient.isDuetClient(), destClient.getHashingAlgorithm(), destClient.shouldEncodeHashKey(), destClient.getMaxDigestBytes(), destClient.getMaxHashLength(), destClient.getBaseEncoder()), readData, ttlToSet, latch);
                }
            }
            latch.await(_operationTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.error("Error fixing up the data.", e);
        }

        // We still do our best to fixup, but client should know the key exist before the Add
        if (fixupAsFail) {
            for (int i = 0; i < destClients.length; i++) {
                prevLatch.countDown();
            }
            if (log.isDebugEnabled()) log.debug("Fixup treated as fail. latchSuccess = " + prevLatch.getSuccessCount() + ", latchExpected = " + prevLatch.getExpectedSuccessCount());
            return prevLatch;
        }
        return latch;
    }

    /**
     * Check if a given key is valid to transmit.
     *
     * @param key the key to check.
     * @param binary if binary protocol is used.
     */
    public static void validateKey(final String key, final boolean binary) {
        // This is a replica of the upstream StringUtils.validateKey from the
        // memcached implementation, however we modify it to avoid the Charset
        // lookup cost, which ends up the majority of the validation.

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int keyLength = keyBytes.length;

        if (keyLength > MAX_KEY_BYTES) {
            throw KEY_TOO_LONG_EXCEPTION;
        }

        if (keyLength == 0) {
            throw KEY_EMPTY_EXCEPTION;
        }

        if(!binary) {
            for (byte b : keyBytes) {
                if (b == ' ' || b == '\n' || b == '\r' || b == 0) {
                    throw new IllegalArgumentException(
                            "Key contains invalid characters:  ``" + key + "''");
                }
            }
        }
    }

}