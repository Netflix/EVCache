package com.netflix.evcache.event;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.pool.EVCacheClient;

import net.spy.memcached.CachedData;

public class EVCacheEvent {

    public static final String CLIENTS = "clients";

    private final Call call;
    private final String appName;
    private final String cacheName;

    private Collection<EVCacheClient> clients = null;
    private Collection<String> keys = null;
    private Collection<String> canonicalKeys = null;
    private int ttl = 0;
    private EVCacheLatch latch = null;
    private CachedData cachedData = null;

    private Map<Object, Object> data;

    public EVCacheEvent(Call call, String appName, String cacheName) {
        super();
        this.call = call;
        this.appName = appName;
        this.cacheName = cacheName;
    }

    public Call getCall() {
        return call;
    }

    public String getAppName() {
        return appName;
    }

    public String getCacheName() {
        return cacheName;
    }

    public Collection<String> getKeys() {
        return keys;
    }

    public void setKeys(Collection<String> keys) {
        this.keys = keys;
    }

    public Collection<String> getCanonicalKeys() {
        return canonicalKeys;
    }

    public void setCanonicalKeys(Collection<String> canonicalKeys) {
        this.canonicalKeys = canonicalKeys;
    }

    public int getTTL() {
        return ttl;
    }

    public void setTTL(int ttl) {
        this.ttl = ttl;
    }

    public EVCacheLatch getLatch() {
        return latch;
    }

    public void setLatch(EVCacheLatch latch) {
        this.latch = latch;
    }

    public CachedData getCachedData() {
        return cachedData;
    }

    public void setCachedData(CachedData cachedData) {
        this.cachedData = cachedData;
    }

    public Collection<EVCacheClient> getClients() {
        return clients;
    }

    public void setClients(Collection<EVCacheClient> clients) {
        this.clients = clients;
    }

    public void setAttribute(Object key, Object value) {
        if (data == null) data = new HashMap<Object, Object>();
        data.put(key, value);
    }

    public Object getAttribute(Object key) {
        if (data == null) return null;
        return data.get(key);
    }

    @Override
    public String toString() {
        return "EVCacheEvent [call=" + call + ", appName=" + appName + ", cacheName=" + cacheName + ", clients="
                + clients + ", keys=" + keys + ", canonicalKeys=" + canonicalKeys + ", ttl=" + ttl + ", latch=" + latch
                + ", cachedData=" + cachedData + ", data=" + data + "]";
    }

}
