package com.netflix.evcache;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.evcache.util.EVCacheConfig;
import net.spy.memcached.CachedData;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EVCacheSerializingTranscoderTest {

    private EVCacheSerializingTranscoder buildTranscoder(String algo, Integer level) {
        DefaultSettableConfig config = new DefaultSettableConfig();
        config.setProperty("test.algo", algo);
        if (level != null) config.setProperty("test.level", level);
        PropertyRepository repo = new DefaultPropertyFactory(config);
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        t.setCompressionAlgorithmProperty(repo.get("test.algo", String.class));
        t.setCompressionLevelProperty(repo.get("test.level", Integer.class));
        return t;
    }

    @Test
    public void testEnumValues() {
        assertEquals(EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("GZIP"),
                EVCacheSerializingTranscoder.CompressionAlgorithm.GZIP);
        assertEquals(EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("ZSTD"),
                EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
    }

    @Test
    public void testDefaultZstdLevelConstant() {
        assertEquals(EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL, 3);
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
        // Zstd magic is 0xFD2FB528 in little-endian: bytes 0x28 0xB5 0x2F 0xFD
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
        EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 0);
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
        DefaultSettableConfig testConfig = new DefaultSettableConfig();
        testConfig.setProperty("default.evcache.compression.algo", "ZSTD");
        testConfig.setProperty("default.evcache.compression.zstd.level",
                EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL);
        PropertyRepository savedRepo = EVCacheConfig.getInstance().getPropertyRepository();
        EVCacheConfig.setPropertyRepository(new DefaultPropertyFactory(testConfig));
        try {
            EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 1);
            String original = "hello world hello world hello world hello world hello world";
            CachedData encoded = transcoder.encode(original);
            String decoded = (String) transcoder.decode(encoded);
            assertEquals(decoded, original);
        } finally {
            EVCacheConfig.setPropertyRepository(savedRepo);
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidAlgorithmEnumThrows() {
        EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("INVALID");
    }

    @Test
    public void testFPAlgorithmGzip() {
        DefaultSettableConfig testConfig = new DefaultSettableConfig();
        testConfig.setProperty("default.evcache.compression.algo", "GZIP");
        PropertyRepository savedRepo = EVCacheConfig.getInstance().getPropertyRepository();
        EVCacheConfig.setPropertyRepository(new DefaultPropertyFactory(testConfig));
        try {
            EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 1);
            CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
            assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                    "COMPRESSED flag must be set");
            byte[] data = encoded.getData();
            assertEquals(data[0], (byte) 0x1f, "FP GZIP must produce gzip magic byte 0");
            assertEquals(data[1], (byte) 0x8b, "FP GZIP must produce gzip magic byte 1");
        } finally {
            EVCacheConfig.setPropertyRepository(savedRepo);
        }
    }

    @Test
    public void testFPAlgorithmZstd() {
        DefaultSettableConfig testConfig = new DefaultSettableConfig();
        testConfig.setProperty("default.evcache.compression.algo", "ZSTD");
        PropertyRepository savedRepo = EVCacheConfig.getInstance().getPropertyRepository();
        EVCacheConfig.setPropertyRepository(new DefaultPropertyFactory(testConfig));
        try {
            EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 1);
            CachedData encoded = transcoder.encode("hello world hello world hello world hello world hello world");
            assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                    "COMPRESSED flag must be set");
            byte[] data = encoded.getData();
            assertEquals(data[0], (byte) 0x28, "FP ZSTD must produce zstd magic byte 0");
            assertEquals(data[1], (byte) 0xB5, "FP ZSTD must produce zstd magic byte 1");
        } finally {
            EVCacheConfig.setPropertyRepository(savedRepo);
        }
    }

    @Test
    public void testFPZstdLevel() {
        DefaultSettableConfig testConfig = new DefaultSettableConfig();
        testConfig.setProperty("default.evcache.compression.algo", "ZSTD");
        testConfig.setProperty("default.evcache.compression.zstd.level", 1);
        PropertyRepository savedRepo = EVCacheConfig.getInstance().getPropertyRepository();
        EVCacheConfig.setPropertyRepository(new DefaultPropertyFactory(testConfig));
        try {
            EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 1);
            String original = "hello world hello world hello world hello world hello world";
            CachedData encoded = transcoder.encode(original);
            String decoded = (String) transcoder.decode(encoded);
            assertEquals(decoded, original, "FP zstd level 1 round-trip must succeed");
        } finally {
            EVCacheConfig.setPropertyRepository(savedRepo);
        }
    }
}
