package com.netflix.evcache.pool;

import java.io.Serializable;
import java.util.Arrays;

public class EVCacheValue implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 3182483105524224448L;

    private final String key;
    private final byte[] value;
    private final int flags;
    private final long ttl;
    private final long createTime;
    public EVCacheValue(String key, byte[] value, int flags, long ttl, long createTime) {
        super();
        this.key = key;
        this.value = value;
        this.flags = flags;
        this.ttl = ttl;
        this.createTime = createTime;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public int getFlags() {
        return flags;
    }

    public long getTTL() {
        return ttl;
    }

    public long getCreateTimeUTC() {
        return createTime;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (createTime ^ (createTime >>> 32));
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + (int) (ttl ^ (ttl >>> 32));
        result = prime * result + (int) (flags);
        result = prime * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVCacheValue other = (EVCacheValue) obj;
        if (createTime != other.createTime)
            return false;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        if (flags != other.flags)
            return false;
        if (ttl != other.ttl)
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EVCacheValue [key=" + key + ", value=" + Arrays.toString(value) + ", flags=" + flags + ", ttl=" + ttl + ", createTime="
                + createTime + "]";
    }

}
