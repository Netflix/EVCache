package com.netflix.evcache;

import net.spy.memcached.CachedData;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class EVCacheSerializingTranscoderTest {

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
    public void testDefaultConstructorUsesGzip() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder();
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x1f, "Default constructor must use gzip");
        assertEquals(data[1], (byte) 0x8b, "Default constructor must use gzip");
    }

    @Test
    public void testSetCompressionAlgorithmProducesZstd() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        t.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        t.setCompressionThreshold(0);
        CachedData encoded = t.encode("hello world hello world hello world hello world hello world");
        assertTrue((encoded.getFlags() & EVCacheSerializingTranscoder.COMPRESSED) != 0,
                "COMPRESSED flag must be set");
        byte[] data = encoded.getData();
        assertEquals(data[0], (byte) 0x28, "setCompressionAlgorithm(ZSTD) must produce zstd magic byte 0");
        assertEquals(data[1], (byte) 0xB5, "setCompressionAlgorithm(ZSTD) must produce zstd magic byte 1");
    }

    @Test
    public void testSetCompressionLevelRoundTrip() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        t.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        t.setCompressionLevel(5);
        t.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = t.encode(original);
        String decoded = (String) t.decode(encoded);
        assertEquals(decoded, original, "Round-trip must succeed with custom zstd level 5");
    }

    @Test
    public void testGzipEncodeSetsGzipMagicBytes() {
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
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
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        t.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
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
        EVCacheSerializingTranscoder transcoder = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        transcoder.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testZstdRoundTrip() {
        EVCacheSerializingTranscoder transcoder = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        transcoder.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        transcoder.setCompressionThreshold(1);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testGzipTranscoderDecodesZstdData() {
        // zstd transcoder writes, gzip transcoder reads → cross-decode via magic-byte detection
        EVCacheSerializingTranscoder writer = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        writer.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        writer.setCompressionThreshold(1);
        EVCacheSerializingTranscoder reader = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);

        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = writer.encode(original);
        String decoded = (String) reader.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testZstdTranscoderDecodesGzipData() {
        // gzip transcoder writes, zstd transcoder reads → cross-decode via magic-byte detection
        EVCacheSerializingTranscoder writer = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        writer.setCompressionThreshold(1);
        EVCacheSerializingTranscoder reader = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        reader.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);

        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = writer.encode(original);
        String decoded = (String) reader.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testEVCacheTranscoderDefaultsToGzip() {
        EVCacheTranscoder transcoder = new EVCacheTranscoder();
        transcoder.setCompressionThreshold(0);
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
    public void testEVCacheTranscoderExplicitAlgorithm() {
        EVCacheTranscoder transcoder = new EVCacheTranscoder(CachedData.MAX_SIZE, 1);
        transcoder.setCompressionAlgorithm(EVCacheSerializingTranscoder.CompressionAlgorithm.ZSTD);
        transcoder.setCompressionLevel(EVCacheSerializingTranscoder.DEFAULT_ZSTD_COMPRESSION_LEVEL);
        String original = "hello world hello world hello world hello world hello world";
        CachedData encoded = transcoder.encode(original);
        String decoded = (String) transcoder.decode(encoded);
        assertEquals(decoded, original);
    }

    @Test
    public void testUncompressedDataPassesThroughDecompress() {
        // backward compat: data with no known magic bytes is returned as-is (not an error)
        EVCacheSerializingTranscoder t = new EVCacheSerializingTranscoder(CachedData.MAX_SIZE);
        byte[] raw = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] result = t.decompress(raw);
        assertEquals(result, raw, "Unrecognized data must be returned unchanged");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidAlgorithmEnumThrows() {
        EVCacheSerializingTranscoder.CompressionAlgorithm.valueOf("INVALID");
    }
}
