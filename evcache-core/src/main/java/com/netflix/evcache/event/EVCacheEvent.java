package com.netflix.evcache.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.netflix.evcache.EVCache.Call;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.EVCacheKey;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;

import net.spy.memcached.CachedData;

public class EVCacheEvent {

    public static final String CLIENTS = "clients";

    private final Call call;
    private final String appName;
    private final String cacheName;
    private final EVCacheClientPool pool;
    private final long startTime;

    private long endTime = 0;
    private String status = EVCacheMetricsFactory.SUCCESS;

    private Collection<EVCacheClient> clients = null;
    private Collection<EVCacheKey> evcKeys = null;
    private int ttl = 0;
    private CachedData cachedData = null;

    private Map<Object, Object> data;

    public EVCacheEvent(Call call, String appName, String cacheName, EVCacheClientPool pool) {
        super();
        this.call = call;
        this.appName = appName;
        this.cacheName = cacheName;
        this.pool = pool;
        this.startTime = System.currentTimeMillis();
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

    public Collection<EVCacheKey> getEVCacheKeys() {
        return evcKeys;
    }

    public void setEVCacheKeys(Collection<EVCacheKey> evcacheKeys) {
        this.evcKeys = evcacheKeys;
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

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    /*
     * Will return the duration of the call if available else -1
     */
    public long getDurationInMillis() {
        if(endTime == 0) return -1;
        return endTime - startTime;
    }

    @Override
    public int hashCode() {
        return evcKeys.hashCode();
    }

    /**
     * @deprecated  replaced by {@link #getEVCacheKeys()}
     */
    @Deprecated
    public Collection<String> getKeys() {
        if(evcKeys == null || evcKeys.size() == 0) return Collections.<String>emptyList();

        final Collection<String> keyList = new ArrayList<String>(evcKeys.size());
        for(EVCacheKey key : evcKeys) {
            keyList.add(key.getKey());
        }
        return keyList;
    }

    /**
     * @deprecated  replaced by {@link #setEVCacheKeys(Collection)}
     */
    @Deprecated 
    public void setKeys(Collection<String> keys) {
    }

    /**
     * @deprecated  replaced by {@link #getEVCacheKeys()}
     */
    @Deprecated
    public Collection<String> getCanonicalKeys() {
        if(evcKeys == null || evcKeys.size() == 0) return Collections.<String>emptyList();

        final Collection<String> keyList = new ArrayList<String>(evcKeys.size());
        for(EVCacheKey key : evcKeys) {
            keyList.add(key.getCanonicalKey());
        }
        return keyList;
    }

    /**
     * @deprecated  replaced by {@link #setEVCacheKeys(Collection)}
     */
    public void setCanonicalKeys(Collection<String> canonicalKeys) {
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
        if (evcKeys == null) {
            if (other.evcKeys != null)
                return false;
        } else if (!evcKeys.equals(other.evcKeys))
            return false;
        return true;
    }

    public long getStartTime() {
        return this.startTime;
    }

    @Override
    public String toString() {
        return "EVCacheEvent [call=" + call + ", appName=" + appName + ", cacheName=" + cacheName + ", Num of Clients="
                + clients.size() + ", evcKeys=" + evcKeys + ", ttl=" + ttl + ", event Time=" + (new Date(startTime)).toString()
                + ", cachedData=" + (cachedData != null ? "[ Flags : " + cachedData.getFlags() + "; Data Array length : " +cachedData.getData().length + "] " : "null") 
                + ", Attributes=" + data + "]";
    }

}
