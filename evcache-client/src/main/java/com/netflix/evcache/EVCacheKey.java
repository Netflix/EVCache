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
    public String toString() {
        return "EVCacheKey [key=" + key + ", canonicalKey=" + canonicalKey + hashKey != null ? ", hashKey=" + hashKey + "]" : "]";
    }

}