package com.netflix.evcache;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.util.KeyHasher;
import com.netflix.evcache.util.KeyHasher.HashingAlgorithm;
import java.util.HashMap;
import java.util.Map;

public class EVCacheKey {
    private final String appName;
    private final HashingAlgorithm hashingAlgorithmAtAppLevel;
    private final Property<Boolean> shouldEncodeHashKeyAtAppLevel;
    private final Property<Integer> maxHashingBytesAtAppLevel;
    private final String key;
    private final String canonicalKey;
    private String canonicalKeyForDuet;
    private final Map<HashingAlgorithm, String> hashedKeysByAlgorithm;
    private final Map<HashingAlgorithm, String> hashedKeysByAlgorithmForDuet;

    public EVCacheKey(String appName, String key, String canonicalKey, HashingAlgorithm hashingAlgorithmAtAppLevel, Property<Boolean> shouldEncodeHashKeyAtAppLevel, Property<Integer> maxHashingBytesAtAppLevel) {
        super();
        this.appName = appName;
        this.key = key;
        this.canonicalKey = canonicalKey;
        this.hashingAlgorithmAtAppLevel = hashingAlgorithmAtAppLevel;
        this.shouldEncodeHashKeyAtAppLevel = shouldEncodeHashKeyAtAppLevel;
        this.maxHashingBytesAtAppLevel = maxHashingBytesAtAppLevel;
        hashedKeysByAlgorithm = new HashMap<>();
        hashedKeysByAlgorithmForDuet = new HashMap<>();
    }

    public String getKey() {
        return key;
    }

    @Deprecated
    public String getCanonicalKey() {
        return canonicalKey;
    }

    public String getCanonicalKey(boolean isDuet) {
        return isDuet ? getCanonicalKeyForDuet() : canonicalKey;
    }

    private String getCanonicalKeyForDuet() {
        if (null == canonicalKeyForDuet) {
            final int duetKeyLength = appName.length() + 1 + canonicalKey.length();
            canonicalKeyForDuet = new StringBuilder(duetKeyLength).append(appName).append(':').append(canonicalKey).toString();
        }

        return canonicalKeyForDuet;
    }

    @Deprecated
    public String getHashKey() {
        return getHashKey(hashingAlgorithmAtAppLevel, null == shouldEncodeHashKeyAtAppLevel ? null : shouldEncodeHashKeyAtAppLevel.get(), null == maxHashingBytesAtAppLevel ? null : maxHashingBytesAtAppLevel.get());
    }

    // overlays app level hashing and client level hashing
    public String getHashKey(boolean isDuet, HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxHashingBytes) {
        if (hashingAlgorithm == HashingAlgorithm.NO_HASHING) {
            return null;
        }

        if (null == hashingAlgorithm) {
            hashingAlgorithm = hashingAlgorithmAtAppLevel;
        }

        if (null == shouldEncodeHashKey) {
            shouldEncodeHashKey = this.shouldEncodeHashKeyAtAppLevel.get();
        }

        if (null == maxHashingBytes) {
            maxHashingBytes = this.maxHashingBytesAtAppLevel.get();
        }

        return isDuet ? getHashKeyForDuet(hashingAlgorithm, shouldEncodeHashKey, maxHashingBytes) : getHashKey(hashingAlgorithm, shouldEncodeHashKey, maxHashingBytes);
    }

    // overlays app level hashing algorithm and client level hashing algorithm
    public String getDerivedKey(boolean isDuet, HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxHashingBytes) {
        // this overlay of hashingAlgorithm helps determine if there at all needs to be hashing performed, otherwise, will return canonical key
        if (null == hashingAlgorithm) {
            hashingAlgorithm = hashingAlgorithmAtAppLevel;
        }

        return null == hashingAlgorithm || hashingAlgorithm == HashingAlgorithm.NO_HASHING ? getCanonicalKey(isDuet) : getHashKey(isDuet, hashingAlgorithm, shouldEncodeHashKey, maxHashingBytes);
    }

    private String getHashKey(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxHashingBytes) {
        if (null == hashingAlgorithm) {
            return null;
        }

        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return hashedKeysByAlgorithm.computeIfAbsent(hashingAlgorithm, ha -> KeyHasher.getHashedKeyEncoded(canonicalKey, ha, maxHashingBytes));
    }

    private String getHashKeyForDuet(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxHashingBytes) {
        if (null == hashingAlgorithm) {
            return null;
        }

        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return hashedKeysByAlgorithmForDuet.computeIfAbsent(hashingAlgorithm, ha -> KeyHasher.getHashedKeyEncoded(getCanonicalKeyForDuet(), ha, maxHashingBytes));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((canonicalKey == null) ? 0 : canonicalKey.hashCode());
        result = prime * result + ((canonicalKeyForDuet == null) ? 0 : canonicalKeyForDuet.hashCode());
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
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EVCacheKey [key=" + key + ", canonicalKey=" + canonicalKey + ", canonicalKeyForDuet=" + canonicalKeyForDuet + (hashedKeysByAlgorithm.size() > 0 ? ", hashedKeysByAlgorithm=" + hashedKeysByAlgorithm.toString() : "") + (hashedKeysByAlgorithmForDuet.size() > 0 ? ", hashedKeysByAlgorithmForDuet=" + hashedKeysByAlgorithmForDuet.toString() + "]" : "]");
    }

}