package com.netflix.evcache.event;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;

import net.spy.memcached.CachedData;

public class EVCacheEvent {

    public static final String CLIENTS = "clients";

    private final Call call;
    private final String appName;
    private final String cacheName;
    private final EVCacheClientPool pool;

    private Collection<EVCacheClient> clients = null;
    private Collection<String> keys = null;
    private Collection<String> canonicalKeys = null;
    private int ttl = 0;
    private CachedData cachedData = null;

    private Map<Object, Object> data;

    public EVCacheEvent(Call call, String appName, String cacheName, EVCacheClientPool pool) {
        super();
        this.call = call;
        this.appName = appName;
        this.cacheName = cacheName;
        this.pool = pool;
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

    public EVCacheClientPool getEVCacheClientPool() {
        return pool;
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
    public int hashCode() {
        return canonicalKeys.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVCacheEvent other = (EVCacheEvent) obj;
        if (appName == null) {
            if (other.appName != null)
                return false;
        } else if (!appName.equals(other.appName))
            return false;
        if (cacheName == null) {
            if (other.cacheName != null)
                return false;
        } else if (!cacheName.equals(other.cacheName))
            return false;
        if (call != other.call)
            return false;
        if (canonicalKeys == null) {
            if (other.canonicalKeys != null)
                return false;
        } else if (!canonicalKeys.equals(other.canonicalKeys))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EVCacheEvent [call=" + call + ", appName=" + appName + ", cacheName=" + cacheName + ", Num of Clients="
                + clients.size() + ", keys=" + keys + ", canonicalKeys=" + canonicalKeys + ", ttl=" + ttl 
                + ", cachedData=" + (cachedData != null ? "[ Flags : " + cachedData.getFlags() + "; Data Array length : " +cachedData.getData().length + "] " : "null") 
                + ", Attributes=" + data + "]";
    }

}
