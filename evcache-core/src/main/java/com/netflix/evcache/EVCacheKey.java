package com.netflix.evcache;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.util.KeyHasher;
import com.netflix.evcache.util.KeyHasher.HashingAlgorithm;

public class EVCacheKey {
    private static final Logger log = LoggerFactory.getLogger(EVCacheKey.class);
    private final String appName;
    private final HashingAlgorithm hashingAlgorithmAtAppLevel;
    private final Property<Boolean> shouldEncodeHashKeyAtAppLevel;
    private final Property<Integer> maxDigestBytesAtAppLevel;
    private final Property<Integer> maxHashLengthAtAppLevel;
    private final String key;
    private final String canonicalKey;
    private String canonicalKeyForDuet;
    // Hashed keys are cached by hashing algorithm, max digest bytes, max hash length and encoder, so clients
    // (server groups) with different hashing properties each get a hash computed with their own properties.
    private final Map<String, String> hashedKeysByAlgorithm;
    private final Map<String, String> hashedKeysByAlgorithmForDuet;
    private final String encoder;

    public EVCacheKey(String appName, String key, String canonicalKey, HashingAlgorithm hashingAlgorithmAtAppLevel, Property<Boolean> shouldEncodeHashKeyAtAppLevel, Property<Integer> maxDigestBytesAtAppLevel, Property<Integer> maxHashLengthAtAppLevel) {
        this(appName, key, canonicalKey, hashingAlgorithmAtAppLevel, shouldEncodeHashKeyAtAppLevel, maxDigestBytesAtAppLevel, maxHashLengthAtAppLevel, null);
    }

    public EVCacheKey(String appName, String key, String canonicalKey, HashingAlgorithm hashingAlgorithmAtAppLevel, Property<Boolean> shouldEncodeHashKeyAtAppLevel, Property<Integer> maxDigestBytesAtAppLevel, Property<Integer> maxHashLengthAtAppLevel, String encoder) {
        super();
        this.appName = appName;
        this.key = key;
        this.canonicalKey = canonicalKey;
        this.hashingAlgorithmAtAppLevel = hashingAlgorithmAtAppLevel;
        this.shouldEncodeHashKeyAtAppLevel = shouldEncodeHashKeyAtAppLevel;
        this.maxDigestBytesAtAppLevel = maxDigestBytesAtAppLevel;
        this.maxHashLengthAtAppLevel = maxHashLengthAtAppLevel;
        this.encoder = encoder;
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
            if (log.isDebugEnabled()) log.debug("canonicalKeyForDuet : " + canonicalKeyForDuet);
        }

        return canonicalKeyForDuet;
    }

    @Deprecated
    public String getHashKey() {
        return getHashKey(hashingAlgorithmAtAppLevel, null == shouldEncodeHashKeyAtAppLevel ? null : shouldEncodeHashKeyAtAppLevel.get(), null == maxDigestBytesAtAppLevel ? null : maxDigestBytesAtAppLevel.get(), null == maxHashLengthAtAppLevel ? null : maxHashLengthAtAppLevel.get(), encoder);
    }

    // overlays app level hashing and client level hashing
    public String getHashKey(boolean isDuet, HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String baseEnoder) {
        if (hashingAlgorithm == HashingAlgorithm.NO_HASHING) {
            return null;
        }

        if (null == hashingAlgorithm) {
            hashingAlgorithm = hashingAlgorithmAtAppLevel;
        }

        if (null == shouldEncodeHashKey) {
            shouldEncodeHashKey = this.shouldEncodeHashKeyAtAppLevel.get();
        }

        if (null == maxDigestBytes) {
            maxDigestBytes = this.maxDigestBytesAtAppLevel.get();
        }

        if (null == maxHashLength) {
            maxHashLength = this.maxHashLengthAtAppLevel.get();
        }
        
        if(null == baseEnoder) {
            baseEnoder = encoder;
        }

        final String rKey = isDuet ? getHashKeyForDuet(hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder) : getHashKey(hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder);
        if (log.isDebugEnabled()) log.debug("Key : " + rKey);
        return rKey;
    }

    // overlays app level hashing algorithm and client level hashing algorithm
    public String getDerivedKey(boolean isDuet, HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String baseEnoder) {
        // this overlay of hashingAlgorithm helps determine if there at all needs to be hashing performed, otherwise, will return canonical key
        if (null == hashingAlgorithm) {
            hashingAlgorithm = hashingAlgorithmAtAppLevel;
        }

        final String derivedKey = null == hashingAlgorithm || hashingAlgorithm == HashingAlgorithm.NO_HASHING ? getCanonicalKey(isDuet) : getHashKey(isDuet, hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder);
        if (log.isDebugEnabled()) log.debug("derivedKey : " + derivedKey);
        return derivedKey;
    }

    private String getHashKey(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String encoder) {
        if (null == hashingAlgorithm) {
            return null;
        }

        final String key = getHashCacheKey(hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
        String val = hashedKeysByAlgorithm.get(key);
        if(val == null) {
            val = KeyHasher.getHashedKeyEncoded(getCanonicalKey(false), hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
            hashedKeysByAlgorithm.put(key , val);
        }
        if (log.isDebugEnabled()) log.debug("getHashKey : " + val);
        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return val;
    }

    private String getHashKeyForDuet(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String encoder) {
        if (null == hashingAlgorithm) {
            return null;
        }

        final String key = getHashCacheKey(hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
        String val = hashedKeysByAlgorithmForDuet.get(key);
        if(val == null) {
            val = KeyHasher.getHashedKeyEncoded(getCanonicalKeyForDuet(), hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
            hashedKeysByAlgorithmForDuet.put(key , val);
        }
        if (log.isDebugEnabled()) log.debug("getHashKeyForDuet : " + val);
        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return val;
    }

    private static String getHashCacheKey(HashingAlgorithm hashingAlgorithm, Integer maxDigestBytes, Integer maxHashLength, String encoder) {
        return hashingAlgorithm + ":" + maxDigestBytes + ":" + maxHashLength + ":" + encoder;
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