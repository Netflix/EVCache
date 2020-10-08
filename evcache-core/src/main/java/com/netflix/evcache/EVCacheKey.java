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
    private final Property<Integer> maxDigestBytesAtAppLevel;
    private final Property<Integer> maxHashLengthAtAppLevel;
    private final String key;
    private final String canonicalKey;
    private String canonicalKeyForDuet;
    // Note that this we cache hashed keys based on Hashing Algorithm alone, but not based on other hashing properties
    // like max.hash.length. So changing max.hash.length alone would not necessarily trigger hash recalculation, but
    // one would have to change the hashing algorithm in order to having hashing properties taken into account.
    // This is to make such a hashing property change very obvious and not subtle.
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

        return isDuet ? getHashKeyForDuet(hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder) : getHashKey(hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder);
    }

    // overlays app level hashing algorithm and client level hashing algorithm
    public String getDerivedKey(boolean isDuet, HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String baseEnoder) {
        // this overlay of hashingAlgorithm helps determine if there at all needs to be hashing performed, otherwise, will return canonical key
        if (null == hashingAlgorithm) {
            hashingAlgorithm = hashingAlgorithmAtAppLevel;
        }

        return null == hashingAlgorithm || hashingAlgorithm == HashingAlgorithm.NO_HASHING ? getCanonicalKey(isDuet) : getHashKey(isDuet, hashingAlgorithm, shouldEncodeHashKey, maxDigestBytes, maxHashLength, baseEnoder);
    }

    private String getHashKey(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String encoder) {
        if (null == hashingAlgorithm) {
            return null;
        }

        final String key = hashingAlgorithm.toString()+ maxDigestBytes != null ? maxDigestBytes.toString() : "-" + maxHashLength != null ? maxHashLength.toString() : "-" + encoder != null ? encoder : "-";
        String val = hashedKeysByAlgorithm.get(key);
        if(val == null) {
            val = KeyHasher.getHashedKeyEncoded(getCanonicalKeyForDuet(), hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
            hashedKeysByAlgorithm.put(key , val);
        }
        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return val;
    }

    private String getHashKeyForDuet(HashingAlgorithm hashingAlgorithm, Boolean shouldEncodeHashKey, Integer maxDigestBytes, Integer maxHashLength, String encoder) {
        if (null == hashingAlgorithm) {
            return null;
        }

        final String key = hashingAlgorithm.toString()+ maxDigestBytes != null ? maxDigestBytes.toString() : "-" + maxHashLength != null ? maxHashLength.toString() : "-" + encoder != null ? encoder : "-";
        String val = hashedKeysByAlgorithmForDuet.get(key);
        if(val == null) {
            val = KeyHasher.getHashedKeyEncoded(getCanonicalKeyForDuet(), hashingAlgorithm, maxDigestBytes, maxHashLength, encoder);
            hashedKeysByAlgorithmForDuet.put(key , val);
        }
        // TODO: Once the issue around passing hashedKey in bytes[] is figured, we will start using (nullable) shouldEncodeHashKey, and call KeyHasher.getHashedKeyInBytes() accordingly
        return val;
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