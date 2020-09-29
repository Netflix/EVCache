package com.netflix.evcache;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.evcache.pool.ServerGroup;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * This class is for internal-use only by EVCache components, and is not recommended to be used for any other purpose. EVCache and EVCacheImpl are recommended instead.
 */
class EVCacheInternalImpl extends EVCacheImpl implements EVCacheInternal {
    private static final Logger log = LoggerFactory.getLogger(EVCacheInternalImpl.class);

    public EVCacheItem<CachedData> metaGet(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException {
        return this.metaGetInternal(key, tc, isOriginalKeyHashed);
    }

    public Map<MemcachedNode, CachedValues> metaGetPerClient(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException {
        Map<MemcachedNode, CachedValues> map = new HashMap<>();
        final Map<ServerGroup, List<EVCacheClient>> instancesByZone = _pool.getAllInstancesByZone();
        final Map<ServerGroup, EVCacheClient> instancesWithNull = new HashMap<ServerGroup, EVCacheClient>();
        final EVCacheKey evcKey = getEVCacheKey(key);
        for (ServerGroup sGroup : instancesByZone.keySet()) {
            try {
                for (EVCacheClient client : instancesByZone.get(sGroup)) {
                    EVCacheItem<CachedData> item = getEVCacheItem(client, evcKey, tc, true, false, isOriginalKeyHashed, false);
                    if (log.isDebugEnabled()) log.debug("client : " + client + "; item : " + item);
                    if(item == null) {
                        instancesWithNull.put(sGroup, client);
                    } else {
                        map.put(client.getNodeLocator().getPrimary(key), null == item ? null : new CachedValues(key, item.getData(), item.getItemMetaData()));
                    }
                }
            } catch (Exception e) {
                log.error("Error getting meta data", e);
            }
        }
        if (log.isDebugEnabled()) log.debug("map : " + map);
        if (log.isDebugEnabled()) log.debug("instancesWithNull : " + instancesWithNull);
        if(instancesWithNull.size() > 0 && map.size() > 0) {
            final EVCacheTranscoder transcoder = new EVCacheTranscoder();
            String originalKey = null;
            for(CachedValues vals : map.values()) {
                if (log.isDebugEnabled()) log.debug("vals : " + vals);
                try {
                    Object obj = transcoder.decode(vals.getData());
                    if (log.isDebugEnabled()) log.debug("Obj : " + obj);
                    if(obj instanceof EVCacheValue) {
                        originalKey = ((EVCacheValue)obj).getKey();
                        if (log.isDebugEnabled()) log.debug("original key: " + originalKey);
                        break;
                    }
                } catch(Exception e) {
                    log.error("Exception decoding", e);
                }
            }
            if(originalKey != null) {
                for(ServerGroup sGroup : instancesWithNull.keySet()) {
                    if (log.isDebugEnabled()) log.debug("sGroup : " + sGroup);
                    final EVCacheClient client = instancesWithNull.get(sGroup);
                    if (log.isDebugEnabled()) log.debug("Client : " + client);
                    EVCacheItem<CachedData> item;
                    try {
                        item = getEVCacheItem(client, getEVCacheKey(originalKey), tc, true, false, false, false);
                        if (log.isDebugEnabled()) log.debug("item : " + item);
                        map.put(client.getNodeLocator().getPrimary(originalKey), null == item ? null : new CachedValues(key, item.getData(), item.getItemMetaData()));
                    } catch (Exception e) {
                        log.error("Exception getting meta data using original key - " + originalKey, e);
                    }
                }
            }
        } else if(map.size() == 0) {
            for (ServerGroup sGroup : instancesByZone.keySet()) {
                try {
                    for (EVCacheClient client : instancesByZone.get(sGroup)) {
                        map.put(client.getNodeLocator().getPrimary(key), null);
                    }
                } catch (Exception e) {
                    log.error("Error getting meta data", e);
                }
            }
        }

        if (log.isDebugEnabled()) log.debug("return map : " + map);
        return map;
    }

    public EVCacheItemMetaData metaDebug(String key, boolean isOriginalKeyHashed) throws EVCacheException {
        return this.metaDebugInternal(key, isOriginalKeyHashed);
    }

    public Map<MemcachedNode, EVCacheItemMetaData> metaDebugPerClient(String key, boolean isOriginalKeyHashed) throws EVCacheException {
        Map<MemcachedNode, EVCacheItemMetaData> map = new HashMap<>();
        final Map<ServerGroup, List<EVCacheClient>> instancesByZone = _pool.getAllInstancesByZone();
        final EVCacheKey evcKey = getEVCacheKey(key);
        for (ServerGroup sGroup : instancesByZone.keySet()) {
            try {
                for (EVCacheClient client : instancesByZone.get(sGroup)) {
                    EVCacheItemMetaData itemMetaData = getEVCacheItemMetaData(client, evcKey, true, false, isOriginalKeyHashed);
                    map.put(client.getNodeLocator().getPrimary(key), itemMetaData);
                }
            } catch (Exception e) {
                log.error("Error getting meta data", e);
            }
        }

        return map;
    }

    public Future<Boolean>[] delete(String key, boolean isOriginalKeyHashed) throws EVCacheException {
        return this.deleteInternal(key, isOriginalKeyHashed);
    }

    public EVCacheInternalImpl(String appName, String cacheName, int timeToLive, Transcoder<?> transcoder, boolean enableZoneFallback,
                boolean throwException, EVCacheClientPoolManager poolManager) {
        super(appName, cacheName, timeToLive, transcoder, enableZoneFallback, throwException, poolManager);
    }

    public EVCacheLatch addOrSetToWriteOnly(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException {
        EVCacheClient[] clients = _pool.getWriteOnlyEVCacheClients();
        if (replaceItem)
            return set(key, value, null, timeToLive, policy, clients, 0);
        else
            return add(key, value, null, timeToLive, policy, clients, 0, false);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException {
        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroups, null);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName) throws EVCacheException {
        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroupName, null);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, List<String> destinationIps) throws EVCacheException {
        List<String> serverGroups = new ArrayList<>();
        serverGroups.add(serverGroupName);

        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroups, destinationIps);
    }

    private EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups, List<String> destinationIps) throws EVCacheException {
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = _pool.getAllInstancesByZone();

        List<EVCacheClient> evCacheClients = clientsByServerGroup.entrySet().stream()
                .filter(entry -> serverGroups.contains(entry.getKey().getName()))
                .map(Map.Entry::getValue)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        if (null != destinationIps && !destinationIps.isEmpty()) {
            // identify that evcache client whose primary node is the destination ip for the key being processed
            evCacheClients = evCacheClients.stream().filter(client ->
                    destinationIps.contains(((InetSocketAddress) client.getNodeLocator()
                        .getPrimary(getEVCacheKey(key).getDerivedKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxDigestBytes(), client.getMaxHashLength()))
                        .getSocketAddress()).getAddress().getHostAddress())
            ).collect(Collectors.toList());
        }

        EVCacheClient[] evCacheClientsArray = new EVCacheClient[evCacheClients.size()];
        evCacheClients.toArray(evCacheClientsArray);

        if (replaceItem)
            return this.set(key, value, null, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
        else
            return this.add(key, value, null, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
    }

    public KeyHashedState isKeyHashed(String appName, String serverGroup) {
        PropertyRepository propertyRepository = _poolManager.getEVCacheConfig().getPropertyRepository();
        boolean isKeyHashedAtAppOrAsg = propertyRepository.get(serverGroup + ".hash.key", Boolean.class).orElseGet(appName + ".hash.key").orElse(false).get();
        if (isKeyHashedAtAppOrAsg) {
            return KeyHashedState.YES;
        }

        if (propertyRepository.get(appName + ".auto.hash.keys", Boolean.class).orElseGet("evcache.auto.hash.keys").orElse(false).get()) {
            return KeyHashedState.MAYBE;
        }

        return KeyHashedState.NO;
    }


}
