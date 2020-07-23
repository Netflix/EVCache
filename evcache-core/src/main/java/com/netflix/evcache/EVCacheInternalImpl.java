package com.netflix.evcache;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.operation.EVCacheItem;
import com.netflix.evcache.operation.EVCacheItemMetaData;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.ServerGroup;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class EVCacheInternalImpl extends EVCacheImpl implements EVCacheInternal {
    public EVCacheItem<CachedData> metaGet(String key, Transcoder<CachedData> tc, boolean isOriginalKeyHashed) throws EVCacheException {
        return this.metaGetInternal(key, tc, isOriginalKeyHashed);
    }

    public EVCacheItemMetaData metaDebug(String key, boolean isOriginalKeyHashed) throws EVCacheException {
        return this.metaDebugInternal(key, isOriginalKeyHashed);
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
            return set(key, value, null, timeToLive, policy, clients, clients.length);
        else
            return add(key, value, null, timeToLive, policy, clients, clients.length);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException {
        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroups, null);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName) throws EVCacheException {
        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroupName, null);
    }

    public EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, String destinationIp) throws EVCacheException {
        List<String> serverGroups = new ArrayList<>();
        serverGroups.add(serverGroupName);

        return addOrSet(replaceItem, key, value, timeToLive, policy, serverGroups, destinationIp);
    }

    private EVCacheLatch addOrSet(boolean replaceItem, String key, CachedData value, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups, String destinationIp) throws EVCacheException {
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = _pool.getAllInstancesByZone();

        List<EVCacheClient> evCacheClients = clientsByServerGroup.entrySet().stream()
                .filter(entry -> serverGroups.contains(entry.getKey().getName()))
                .map(Map.Entry::getValue)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        if (null != destinationIp && !destinationIp.isEmpty()) {
            // identify that evcache client whose primary node is the destination ip for the key being processed
            evCacheClients = evCacheClients.stream().filter(client ->
                    ((InetSocketAddress) client.getNodeLocator()
                        .getPrimary(getEVCacheKey(key).getDerivedKey(client.isDuetClient(), client.getHashingAlgorithm(), client.shouldEncodeHashKey(), client.getMaxHashingBytes()))
                        .getSocketAddress()).getAddress().getHostAddress().equals(destinationIp)
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
