package com.netflix.evcache;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.config.EVCacheTranscoderProperties.CompressionAlgorithm;
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

    /**
     * Build a serializing transcoder whose compression algorithm/level are resolved from a
     * fresh {@link EVCacheTranscoderProperties} bundle (global keys, no app prefix).
     */
    private EVCacheSerializingTranscoder buildTranscoder(String algo, Integer level) {
        DefaultSettableConfig config = new DefaultSettableConfig();
        if (algo != null) config.setProperty("default.evcache.compression.algorithm", algo);
        if (level != null) config.setProperty("default.evcache.compression.zstd.level", level);
        EVCacheTranscoderProperties props =
                new EVCacheTranscoderProperties(null, new DefaultPropertyFactory(config));
        return new EVCacheSerializingTranscoder(CachedData.MAX_SIZE, props);
    }

    /**
     * Build an {@link EVCacheTranscoder} from a config, resolved for the given app name, with the
     * compression threshold forced low so the short test payloads always compress.
     */
    private EVCacheTranscoder buildEVCacheTranscoder(String appName, DefaultSettableConfig config, int threshold) {
        EVCacheTranscoder t =
                new EVCacheTranscoder(new EVCacheTranscoderProperties(appName, new DefaultPropertyFactory(config)));
        t.setCompressionThreshold(threshold);
        return t;
    }

    @Test
    public void testEnumValues() {
        assertEquals(CompressionAlgorithm.valueOf("GZIP"), CompressionAlgorithm.GZIP);
        assertEquals(CompressionAlgorithm.valueOf("ZSTD"), CompressionAlgorithm.ZSTD);
    }

    @Test
    public void testDefaultZstdLevelConstant() {
        assertEquals(EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL, 3);
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
        // Zstd magic is 0xFD2FB528 in little-endian: bytes 0x28 0xB5 0x2F 0xFD
        assertEquals(data[0], (byte) 0x28, "Expected zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "Expected zstd magic byte 1");
        assertEquals(data[2], (byte) 0x2F, "Expected zstd magic byte 2");
        assertEquals(data[3], (byte) 0xFD, "Expected zstd magic byte 3");
    }

    @Test
    public void testCompressionThatGrowsDataLeavesCompressedFlagUnset() {
        // encode() only sets COMPRESSED (and keeps the compressed bytes) when compression actually
        // shrinks the payload. Tiny incompressible input grows under gzip framing, so the original
        // bytes must be kept and the COMPRESSED flag must stay clear.
        EVCacheSerializingTranscoder t = buildTranscoder("GZIP", null);
        t.setCompressionThreshold(0);
        byte[] tiny = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        CachedData encoded = t.encode(tiny);
        assertEquals(encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED, 0,
                "COMPRESSED flag must not be set when compression grows the payload");
        assertEquals(encoded.getData(), tiny, "original bytes must be kept when compression does not help");
        assertEquals((byte[]) t.decode(encoded), tiny, "round-trip must return the original bytes");
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
        // zstd transcoder writes, gzip transcoder reads → cross-decode via magic-byte detection
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
        // gzip transcoder writes, zstd transcoder reads → cross-decode via magic-byte detection
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
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, new DefaultSettableConfig(), 0);
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
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "ZSTD");
        config.setProperty("default.evcache.compression.zstd.level",
                EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL);
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, config, 1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testAppNamePrefixedAlgoOverridesDefault() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "GZIP");
        config.setProperty("EVCACHE_TEST.compression.algorithm", "ZSTD");
        EVCacheTranscoder transcoder = buildEVCacheTranscoder("EVCACHE_TEST", config, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "app-specific ZSTD override must win over default GZIP");
        assertEquals(data[1], (byte) 0xB5, "app-specific ZSTD override must win over default GZIP");
    }

    @Test
    public void testAppNameFallsBackToDefaultAlgoWhenNoOverride() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "ZSTD");
        EVCacheTranscoder transcoder = buildEVCacheTranscoder("EVCACHE_NO_OVERRIDE", config, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "must fall back to default ZSTD when no app-specific override exists");
        assertEquals(data[1], (byte) 0xB5, "must fall back to default ZSTD when no app-specific override exists");
    }

    @Test
    public void testAppNamePrefixedZstdLevelRoundTrip() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "ZSTD");
        config.setProperty("default.evcache.compression.zstd.level", 1);
        config.setProperty("EVCACHE_TEST.compression.zstd.level", 5);
        EVCacheTranscoder transcoder = buildEVCacheTranscoder("EVCACHE_TEST", config, 1);
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
            DefaultSettableConfig config = new DefaultSettableConfig();
            config.setProperty("default.evcache.compression.algorithm", "GZIP");
            EVCacheTranscoderProperties props =
                    new EVCacheTranscoderProperties(appName, new DefaultPropertyFactory(config));
            EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE, props);
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
        CompressionAlgorithm.valueOf("INVALID");
    }

    @Test
    public void testFPAlgorithmGzip() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "GZIP");
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, config, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "FP GZIP must produce gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "FP GZIP must produce gzip magic byte 1");
    }

    @Test
    public void testFPAlgorithmZstd() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "ZSTD");
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, config, 1);
        CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "FP ZSTD must produce zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "FP ZSTD must produce zstd magic byte 1");
    }

    @Test
    public void testUnrecognizedAlgorithmFallsBackToGzipAndEncodes() {
        // An unknown FP algorithm value must not NPE encode(); it degrades to the default (GZIP).
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "SNAPPY");
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, config, 1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "unknown algorithm must fall back to gzip magic byte 0");
        assertEquals(data[1], (byte) 0x8b, "unknown algorithm must fall back to gzip magic byte 1");
        assertEquals((String) transcoder.decode(encoded), original, "round-trip must succeed after fallback");
    }

    @Test
    public void testFPZstdLevel() {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("default.evcache.compression.algorithm", "ZSTD");
        config.setProperty("default.evcache.compression.zstd.level", 1);
        EVCacheTranscoder transcoder = buildEVCacheTranscoder(null, config, 1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original, "FP zstd level 1 round-trip must succeed");
    }
}
