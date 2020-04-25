package com.netflix.evcache;

public class EVCacheKey {
    private final String key;
    private final String canonicalKey;
    private final String canonicalKeyForDuet;
    private final String hashKey;
    private final String hashKeyForDuet;

    public EVCacheKey(String key, String canonicalKey, String canonicalKeyForDuet, String hashKey, String hashKeyForDuet) {
        super();
        this.key = key;
        this.canonicalKey = canonicalKey;
        this.canonicalKeyForDuet = canonicalKeyForDuet;
        this.hashKey = hashKey;
        this.hashKeyForDuet = hashKeyForDuet;
    }

    public String getKey() {
        return key;
    }

    @Deprecated
    public String getCanonicalKey() {
        return canonicalKey;
    }

    public String getCanonicalKey(boolean isDuet) {
        return isDuet ? canonicalKeyForDuet : canonicalKey;
    }

    @Deprecated
    public String getHashKey() {
        return hashKey;
    }

    public String getHashKey(boolean isDuet) {
        return isDuet ? hashKeyForDuet : hashKey;
    }
    
    public String getDerivedKey(boolean isDuet)
    {
        if (isDuet)
            return null == hashKeyForDuet ? canonicalKeyForDuet : hashKeyForDuet;

        return null == hashKey ? canonicalKey : hashKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((canonicalKey == null) ? 0 : canonicalKey.hashCode());
        result = prime * result + ((canonicalKeyForDuet == null) ? 0 : canonicalKeyForDuet.hashCode());
        result = prime * result + ((hashKey == null) ? 0 : hashKey.hashCode());
        result = prime * result + ((hashKeyForDuet == null) ? 0 : hashKeyForDuet.hashCode());
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
        if (canonicalKeyForDuet == null) {
            if (other.canonicalKeyForDuet != null)
                return false;
        } else if (!canonicalKeyForDuet.equals(other.canonicalKeyForDuet))
            return false;
        if (hashKey == null) {
            if (other.hashKey != null)
                return false;
        } else if (!hashKey.equals(other.hashKey))
            return false;
        if (hashKeyForDuet == null) {
            if (other.hashKeyForDuet != null)
                return false;
        } else if (!hashKeyForDuet.equals(other.hashKeyForDuet))
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
        return "EVCacheKey [key=" + key + ", canonicalKey=" + canonicalKey + ", canonicalKeyForDuet=" + canonicalKeyForDuet + (hashKey != null ? ", hashKey=" + hashKey : "") + (hashKeyForDuet != null ? ", hashKeyForDuet=" + hashKeyForDuet + "]" : "]");
    }

}