package com.netflix.evcache;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Meter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
import net.spy.memcached.CachedData;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EVCacheSerializingTranscoderTest {

    private static final String GLOBAL_ALGO_KEY = "default.evcache.compression.algorithm";
    private static final String GLOBAL_ZSTD_LEVEL_KEY = "default.evcache.compression.zstd.level";
    private static final String PER_APP_ALGO_SUFFIX = ".compression.algorithm";
    private static final String PER_APP_ZSTD_LEVEL_SUFFIX = ".compression.zstd.level";

    private EVCacheSerializingTranscoder buildTranscoder(String algo, Integer level) {
        return buildTranscoder(null, algo, level);
    }

    private EVCacheSerializingTranscoder buildTranscoder(String appName, String algo, Integer level) {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        if (algo != null) cfg.setProperty(GLOBAL_ALGO_KEY, algo);
        if (level != null) cfg.setProperty(GLOBAL_ZSTD_LEVEL_KEY, level);
        PropertyRepository repo = new DefaultPropertyFactory(cfg);
        return new EVCacheSerializingTranscoder(
                new EVCacheTranscoderProperties(appName, repo), CachedData.MAX_SIZE);
    }

    private EVCacheTranscoder buildEVCacheTranscoder(String appName, String algo, Integer level, int compressionThreshold) {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        if (algo != null) cfg.setProperty(GLOBAL_ALGO_KEY, algo);
        if (level != null) cfg.setProperty(GLOBAL_ZSTD_LEVEL_KEY, level);
        PropertyRepository repo = new DefaultPropertyFactory(cfg);
        EVCacheTranscoder t = new EVCacheTranscoder(new EVCacheTranscoderProperties(appName, repo));
        t.setCompressionThreshold(compressionThreshold);
        return t;
    }

    @Test
    public void testEnumValues() {
        assertEquals(EVCacheTranscoderProperties.CompressionAlgorithm.valueOf("GZIP"),
                EVCacheTranscoderProperties.CompressionAlgorithm.GZIP);
        assertEquals(EVCacheTranscoderProperties.CompressionAlgorithm.valueOf("ZSTD"),
                EVCacheTranscoderProperties.CompressionAlgorithm.ZSTD);
    }

    @Test
    public void testDefaultZstdLevelConstant() {
        assertEquals(EVCacheTranscoderProperties.DEFAULT_COMPRESSION_ZSTD_LEVEL, 3);
    }

    @Test
    public void testGzipDefaultProducesGzipMagicBytes() {
        EVCacheSerializingTranscoder t = buildTranscoder("GZIP", null);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "GZIP property must produce gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "GZIP property must produce gzip magic byte 1");
    }

    @Test
    public void testZstdPropertyProducesZstdMagicBytes() {
        EVCacheSerializingTranscoder t = buildTranscoder("ZSTD", null);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "ZSTD property must produce zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "ZSTD property must produce zstd magic byte 1");
    }

    @Test
    public void testCustomZstdLevelRoundTrip() {
        EVCacheSerializingTranscoder t = buildTranscoder("ZSTD", 5);
        t.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = t.encode(original);
        String decoded = (String) t.decode(encoded);
        assertEquals(decoded, original, "Round-trip must succeed with custom zstd level 5");
    }

    @Test
    public void testGzipEncodeSetsGzipMagicBytes() {
        EVCacheSerializingTranscoder t = buildTranscoder("GZIP", null);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "Expected gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "Expected gzip magic byte 1");
    }

    @Test
    public void testZstdEncodeSetsZstdMagicBytes() {
        EVCacheSerializingTranscoder t = buildTranscoder("ZSTD", null);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "Expected zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "Expected zstd magic byte 1");
        assertEquals(data[2], (byte) 0x2F, "Expected zstd magic byte 2");
        assertEquals(data[3], (byte) 0xFD, "Expected zstd magic byte 3");
    }

    @Test
    public void testGzipRoundTrip() {
        EVCacheSerializingTranscoder transcoder = buildTranscoder("GZIP", null);
        transcoder.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testZstdRoundTrip() {
        EVCacheSerializingTranscoder transcoder = buildTranscoder("ZSTD", null);
        transcoder.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testGzipTranscoderDecodesZstdData() {
        EVCacheSerializingTranscoder writer = buildTranscoder("ZSTD", null);
        writer.setCompressionThreshold(1);
        EVCacheSerializingTranscoder reader = buildTranscoder("GZIP", null);

        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = writer.encode(original);
        String decoded = (String) reader.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testZstdTranscoderDecodesGzipData() {
        EVCacheSerializingTranscoder writer = buildTranscoder("GZIP", null);
        writer.setCompressionThreshold(1);
        EVCacheSerializingTranscoder reader = buildTranscoder("ZSTD", null);

        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = writer.encode(original);
        String decoded = (String) reader.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testEVCacheTranscoderDefaultsToGzip() {
        // No algo property set anywhere -> bundle default is GZIP.
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, null, null, 0);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "EVCacheTranscoder must default to gzip");
        assertEquals(data[1], (byte) 0x8b, "EVCacheTranscoder must default to gzip");
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testEVCacheTranscoderExplicitZstdAlgorithm() {
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, "ZSTD",
                EVCacheTranscoderProperties.DEFAULT_COMPRESSION_ZSTD_LEVEL, 1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testAppNamePrefixedAlgoOverridesDefault() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(GLOBAL_ALGO_KEY, "GZIP");
        cfg.setProperty("EVCACHE_TEST" + PER_APP_ALGO_SUFFIX, "ZSTD");
        PropertyRepository repo = new DefaultPropertyFactory(cfg);
        EVCacheTranscoder transcoder = new EVCacheTranscoder(new EVCacheTranscoderProperties("EVCACHE_TEST", repo));
        transcoder.setCompressionThreshold(1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "app-specific ZSTD override must win over default GZIP");
        assertEquals(data[1], (byte) 0xB5, "app-specific ZSTD override must win over default GZIP");
    }

    @Test
    public void testAppNameFallsBackToDefaultAlgoWhenNoOverride() {
        // Global says ZSTD, per-app for a *different* app; our transcoder falls back to global.
        EVCacheTranscoder transcoder = buildEVCacheTranscoder("EVCACHE_NO_OVERRIDE", "ZSTD", null, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "must fall back to global ZSTD when no app-specific override exists");
        assertEquals(data[1], (byte) 0xB5, "must fall back to global ZSTD when no app-specific override exists");
    }

    @Test
    public void testAppNamePrefixedZstdLevelRoundTrip() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(GLOBAL_ALGO_KEY, "ZSTD");
        cfg.setProperty(GLOBAL_ZSTD_LEVEL_KEY, 1);
        cfg.setProperty("EVCACHE_TEST" + PER_APP_ZSTD_LEVEL_SUFFIX, 5);
        PropertyRepository repo = new DefaultPropertyFactory(cfg);
        EVCacheTranscoder transcoder = new EVCacheTranscoder(new EVCacheTranscoderProperties("EVCACHE_TEST", repo));
        transcoder.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original, "app-specific zstd level override round-trip must succeed");
    }

    @Test
    public void testCompressionRatioMetricTaggedWithAppName() {
        final String appName = "EVCACHE_RATIO_TEST";
        Registry registry = new DefaultRegistry();
        Spectator.globalRegistry().add(registry);
        try {
            EVCacheSerializingTranscoder t = buildTranscoder(appName, "GZIP", null);
            t.setCompressionThreshold(0);
            t.encode("hello world hello world hello world hello world hello world");

            assertTrue(hasCompressionRatioCacheTag(registry, appName),
                    "compression ratio metric must carry the " + EVCacheMetricsFactory.CACHE + " tag with the app name");
        } finally {
            Spectator.globalRegistry().remove(registry);
        }
    }

    @Test
    public void testCompressionRatioMetricNotTaggedWhenNoAppName() {
        Registry registry = new DefaultRegistry();
        Spectator.globalRegistry().add(registry);
        try {
            EVCacheSerializingTranscoder t = buildTranscoder("GZIP", null);
            t.setCompressionThreshold(0);
            t.encode("hello world hello world hello world hello world hello world");

            for (Meter meter : registry) {
                Id id = meter.id();
                if (EVCacheMetricsFactory.COMPRESSION_RATIO.equals(id.name())) {
                    for (Tag tag : id.tags()) {
                        assertNotEquals(tag.key(), EVCacheMetricsFactory.CACHE,
                                "no app name tag must be added when app name is absent");
                    }
                }
            }
        } finally {
            Spectator.globalRegistry().remove(registry);
        }
    }

    private boolean hasCompressionRatioCacheTag(Registry registry, String appName) {
        for (Meter meter : registry) {
            Id id = meter.id();
            if (EVCacheMetricsFactory.COMPRESSION_RATIO.equals(id.name())) {
                for (Tag tag : id.tags()) {
                    if (EVCacheMetricsFactory.CACHE.equals(tag.key()) && appName.equals(tag.value())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidAlgorithmEnumThrows() {
        EVCacheTranscoderProperties.CompressionAlgorithm.valueOf("INVALID");
    }

    @Test
    public void testFPAlgorithmGzip() {
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, "GZIP", null, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "FP GZIP must produce gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "FP GZIP must produce gzip magic byte 1");
    }

    @Test
    public void testFPAlgorithmZstd() {
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, "ZSTD", null, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "FP ZSTD must produce zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "FP ZSTD must produce zstd magic byte 1");
    }

    @Test
    public void testFPZstdLevel() {
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, "ZSTD", 1, 1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original, "FP zstd level 1 round-trip must succeed");
    }
}
