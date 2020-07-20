package com.netflix.evcache;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.ServerGroup;
import net.spy.memcached.transcoders.Transcoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EVCacheInternalImpl extends EVCacheImpl implements EVCacheInternal {
    public EVCacheInternalImpl(String appName, String cacheName, int timeToLive, Transcoder<?> transcoder, boolean enableZoneFallback,
                boolean throwException, EVCacheClientPoolManager poolManager) {
        super(appName, cacheName, timeToLive, transcoder, enableZoneFallback, throwException, poolManager);
    }

    public <T> EVCacheLatch addOrSetToWriteOnly(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException {
        EVCacheClient[] clients = _pool.getWriteOnlyEVCacheClients();
        if (replaceItem)
            return set(key, value, tc, timeToLive, policy, clients, clients.length);
        else
            return add(key, value, tc, timeToLive, policy, clients, clients.length);
    }

    public <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, List<String> serverGroups) throws EVCacheException {
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = _pool.getAllInstancesByZone();

        List<EVCacheClient> evCacheClients = clientsByServerGroup.entrySet().stream()
                .filter(entry -> serverGroups.contains(entry.getKey().getName()))
                .map(Map.Entry::getValue)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        EVCacheClient[] evCacheClientsArray = new EVCacheClient[evCacheClients.size()];
        evCacheClients.toArray(evCacheClientsArray);

        if (replaceItem)
            return this.set(key, value, tc, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
        else
            return this.add(key, value, tc, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
    }

    public <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, String serverGroup) throws EVCacheException {
        List<String> serverGroups = new ArrayList<>();
        serverGroups.add(serverGroup);
        return addOrSet(replaceItem, key, value, tc, timeToLive, policy, serverGroups);
    }

    public <T> EVCacheLatch addOrSet(boolean replaceItem, String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy, String serverGroupName, String destinationIp) throws EVCacheException {
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = _pool.getAllInstancesByZone();
        List<EVCacheClient> evCacheClients = clientsByServerGroup.entrySet().stream()
                .filter(entry -> serverGroupName.equalsIgnoreCase(entry.getKey().getName()))
                .map(Map.Entry::getValue)
                .flatMap(List::stream)
                .filter(client -> client.getMemcachedNodesInZone().stream().anyMatch(inetSocketAddress -> inetSocketAddress.getAddress().getHostAddress().equals(destinationIp)))
                .collect(Collectors.toList());

        EVCacheClient[] evCacheClientsArray = new EVCacheClient[evCacheClients.size()];
        evCacheClients.toArray(evCacheClientsArray);

        if (replaceItem)
            return this.set(key, value, tc, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
        else
            return this.add(key, value, tc, timeToLive, policy, evCacheClientsArray, evCacheClientsArray.length);
    }

    public boolean isKeyHashed(String appName, String serverGroup) {
        PropertyRepository propertyRepository = _poolManager.getEVCacheConfig().getPropertyRepository();
        return propertyRepository.get(serverGroup + ".hash.key", Boolean.class).orElseGet(appName + ".hash.key").orElse(false).get();
    }


}
