package com.netflix.evcache;

public class EVCacheKey {
    private final String key;
    private final String canonicalKey;
    private final String hashKey;

    public EVCacheKey(String key, String canonicalKey, String hashKey) {
        super();
        this.key = key;
        this.canonicalKey = canonicalKey;
        this.hashKey = hashKey;
    }

    public String getKey() {
        return key;
    }

    public String getCanonicalKey() {
        return canonicalKey;
    }

    public String getHashKey() {
        return hashKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((canonicalKey == null) ? 0 : canonicalKey.hashCode());
        result = prime * result + ((hashKey == null) ? 0 : hashKey.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
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
        EVCacheKey other = (EVCacheKey) obj;
        if (canonicalKey == null) {
            if (other.canonicalKey != null)
                return false;
        } else if (!canonicalKey.equals(other.canonicalKey))
            return false;
        if (hashKey == null) {
            if (other.hashKey != null)
                return false;
        } else if (!hashKey.equals(other.hashKey))
            return false;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EVCacheKey [key=" + key + ", canonicalKey=" + canonicalKey + (hashKey != null ? ", hashKey=" + hashKey + "]" : "]");
    }

}