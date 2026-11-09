package com.netflix.evcache;

import com.netflix.archaius.api.Property;
import com.netflix.evcache.util.KeyHasher;
import com.netflix.evcache.util.KeyHasher.HashingAlgorithm;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class EVCacheKeyTest {

    private static final String APP = "EVCACHE_TEST";
    private static final String KEY = "key";

    @SuppressWarnings("unchecked")
    private static <T> Property<T> property(T value) {
        Property<T> property = mock(Property.class);
        when(property.get()).thenReturn(value);
        return property;
    }

    private static EVCacheKey newKey() {
        return new EVCacheKey(APP, KEY, KEY, HashingAlgorithm.siphash24, property(Boolean.TRUE),
                property(-1), property(-1), "base64");
    }

    @Test
    public void hashKeyIsRecomputedForEachHashingAlgorithm() {
        EVCacheKey evcKey = newKey();

        String siphash = evcKey.getHashKey(false, HashingAlgorithm.siphash24, null, -1, -1, "base64");
        String md5 = evcKey.getHashKey(false, HashingAlgorithm.md5, null, -1, -1, "base64");

        assertEquals(siphash, KeyHasher.getHashedKeyEncoded(KEY, HashingAlgorithm.siphash24, -1, -1, "base64"));
        assertEquals(md5, KeyHasher.getHashedKeyEncoded(KEY, HashingAlgorithm.md5, -1, -1, "base64"));
        assertNotEquals(md5, siphash);
    }

    @Test
    public void hashKeyIsRecomputedForEachMaxHashLengthAndEncoder() {
        EVCacheKey evcKey = newKey();

        String full = evcKey.getHashKey(false, HashingAlgorithm.md5, null, -1, -1, "base64");
        String truncated = evcKey.getHashKey(false, HashingAlgorithm.md5, null, -1, 8, "base64");
        String ascii85 = evcKey.getHashKey(false, HashingAlgorithm.md5, null, -1, -1, "ascii85");

        assertEquals(full, KeyHasher.getHashedKeyEncoded(KEY, HashingAlgorithm.md5, -1, -1, "base64"));
        assertEquals(truncated, KeyHasher.getHashedKeyEncoded(KEY, HashingAlgorithm.md5, -1, 8, "base64"));
        assertEquals(ascii85, KeyHasher.getHashedKeyEncoded(KEY, HashingAlgorithm.md5, -1, -1, "ascii85"));
    }

    @Test
    public void duetHashKeyIsRecomputedForEachHashingAlgorithm() {
        EVCacheKey evcKey = newKey();
        String duetKey = APP + ":" + KEY;

        String siphash = evcKey.getHashKey(true, HashingAlgorithm.siphash24, null, -1, -1, "base64");
        String md5 = evcKey.getHashKey(true, HashingAlgorithm.md5, null, -1, -1, "base64");

        assertEquals(siphash, KeyHasher.getHashedKeyEncoded(duetKey, HashingAlgorithm.siphash24, -1, -1, "base64"));
        assertEquals(md5, KeyHasher.getHashedKeyEncoded(duetKey, HashingAlgorithm.md5, -1, -1, "base64"));
    }
}
