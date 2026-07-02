package com.netflix.evcache.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import com.netflix.evcache.config.EVCacheTranscoderProperties;
import org.testng.annotations.Test;

import com.netflix.evcache.EVCacheTranscoder;

import net.spy.memcached.CachedData;

/**
 * Pure unit tests for the compact binary serialization of {@link EVCacheValue} (the
 * envelope wire format implemented inside {@link EVCacheTranscoder}), its routing through
 * the transcoder, and backwards-compatibility with the legacy Java-serialized format.
 * All tests go through the public {@link EVCacheTranscoder#encode(Object)} /
 * {@link EVCacheTranscoder#decode(CachedData)} API — the binary codec itself is a private
 * implementation detail of {@link EVCacheTranscoder}. No memcached, no DI.
 */
public class EVCacheValueSerdeTest {

    private static final int SERIALIZED = 1; // EVCacheSerializingTranscoder.SERIALIZED
    private static final byte JAVA_STREAM_MAGIC_FIRST = (byte) 0xAC;
    private static final byte JAVA_STREAM_MAGIC_SECOND = (byte) 0xED;

    // ---- helpers ----

    /** Binary-enabled transcoder, compression disabled, so encoded bytes start with our magic. */
    private static EVCacheTranscoder binaryTranscoder() {
        com.netflix.archaius.config.DefaultSettableConfig cfg = new com.netflix.archaius.config.DefaultSettableConfig();
        cfg.setProperty("testApp.binary.serialization.enabled", "true");
        cfg.setProperty("testApp.compression.threshold", String.valueOf(Integer.MAX_VALUE));

        return new EVCacheTranscoder(
                new EVCacheTranscoderProperties("testApp",
                        com.netflix.archaius.DefaultPropertyFactory.from(cfg)));
    }

    /** Default transcoder (binary OFF, falls through to native Java serialization). */
    private static EVCacheTranscoder defaultTranscoder() {
        return new EVCacheTranscoder(EVCacheTranscoderProperties.DEFAULT_MAX_DATA_SIZE_BYTES, Integer.MAX_VALUE);
    }

    private EVCacheValue value(String key, byte[] val, int flags, long ttl, long createTime) {
        return new EVCacheValue(key, val, flags, ttl, createTime);
    }

    private EVCacheValue typical() {
        return value("myKey", "hello world".getBytes(StandardCharsets.UTF_8), 0, 3600L, 1_700_000_000_000L);
    }

    /** Serialize an object the legacy way an old client would: java.io ObjectOutputStream. */
    private byte[] javaSerialize(Object o) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
        }
        return baos.toByteArray();
    }

    private int javaSerializedLength(EVCacheValue v) throws Exception {
        return javaSerialize(v).length;
    }

    /** End-to-end round-trip via the public transcoder API with binary serialization enabled. */
    private void assertBinaryRoundTrip(EVCacheValue v) {
        EVCacheTranscoder t = binaryTranscoder();
        CachedData cd = t.encode(v);
        // Sanity: actually binary-encoded.
        assertThat(cd.getData()[0]).isEqualTo(EVCacheValueSerde.BINARY_SERDE_MAGIC_CONSTANT_BYTE);
        EVCacheValue out = (EVCacheValue) t.decode(cd);
        assertThat(out).isEqualTo(v);
    }

    // ---- 1. Binary round-trip across cases (via transcoder) ----

    @Test
    public void testBinaryRoundTripEmptyValue() {
        assertBinaryRoundTrip(value("k", new byte[0], 0, 100L, 1L));
    }

    @Test
    public void testBinaryRoundTripLargeValue() {
        byte[] large = new byte[2 * 1024 * 1024];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) (i & 0xFF);
        }
        assertBinaryRoundTrip(value("largeKey", large, 2, 86400L, 1_700_000_000_000L));
    }

    @Test
    public void testBinaryRoundTripUnicodeKey() {
        assertBinaryRoundTrip(value("键🔑-é- key",
                "payload".getBytes(StandardCharsets.UTF_8), 7, 60L, 42L));
    }

    @Test
    public void testBinaryRoundTripZeroTtl() {
        assertBinaryRoundTrip(value("zt", "v".getBytes(StandardCharsets.UTF_8), 1, 0L, 42L));
    }

    @Test
    public void testBinaryRoundTripNegativeCreateTime() {
        assertBinaryRoundTrip(value("nct", "v".getBytes(StandardCharsets.UTF_8), 1, 60L, -987654321L));
    }

    @Test
    public void testBinaryRoundTripMaxCreateTime() {
        assertBinaryRoundTrip(value("mct", "v".getBytes(StandardCharsets.UTF_8), 1, 60L, Long.MAX_VALUE));
    }

    @Test
    public void testBinaryRoundTripMinFlags() {
        assertBinaryRoundTrip(value("minf", "v".getBytes(StandardCharsets.UTF_8), Integer.MIN_VALUE, 60L, 42L));
    }

    // ---- 2. Transcoder produces expected wire shape (binary mode) ----

    @Test
    public void testTranscoderBinaryWireShape() {
        EVCacheTranscoder t = binaryTranscoder();
        EVCacheValue v = typical();

        CachedData cd = t.encode(v);

        // SERIALIZED flag must be set so decode routes through deserialize().
        assertThat(cd.getFlags() & SERIALIZED).isNotZero();
        // Binary envelope marker present (no compression interfering).
        assertThat(cd.getData()[0]).isEqualTo(EVCacheValueSerde.BINARY_SERDE_MAGIC_CONSTANT_BYTE);
        // Byte index 1 is the reserved/version byte, currently always 0x00.
        assertThat(cd.getData()[1]).isEqualTo((byte) 0x00);

        Object out = t.decode(cd);
        assertThat(out).isInstanceOf(EVCacheValue.class);
        assertThat(out).isEqualTo(v);
    }

    // ---- 3. Default transcoder writes Java, but decode reads both formats ----

    @Test
    public void testTranscoderDefaultProducesJavaAndReadsBoth() {
        EVCacheTranscoder t = defaultTranscoder();
        EVCacheValue v = typical();

        CachedData cd = t.encode(v);

        // Java serialization stream magic is 0xAC 0xED.
        byte[] data = cd.getData();
        assertThat(data[0]).isEqualTo(JAVA_STREAM_MAGIC_FIRST);
        assertThat(data[1]).isEqualTo(JAVA_STREAM_MAGIC_SECOND);
        // SERIALIZED flag still set.
        assertThat(cd.getFlags() & SERIALIZED).isNotZero();

        // Dual-format read: default-Java write decodes back to an equal EVCacheValue.
        Object out = t.decode(cd);
        assertThat(out).isInstanceOf(EVCacheValue.class);
        assertThat(out).isEqualTo(v);
    }

    // ---- 4. Backwards-compat: new client reads legacy Java-serialized bytes ----

    @Test
    public void testBackwardsCompatLegacyJavaSerialized() throws Exception {
        EVCacheValue v = typical();
        byte[] javaBytes = javaSerialize(v);

        // Sanity: legacy bytes start with the Java stream header, not our binary magic.
        assertThat(javaBytes[0]).isEqualTo(JAVA_STREAM_MAGIC_FIRST);
        assertThat(javaBytes[0]).isNotEqualTo(EVCacheValueSerde.BINARY_SERDE_MAGIC_CONSTANT_BYTE);

        CachedData cd = new CachedData(SERIALIZED, javaBytes, CachedData.MAX_SIZE);
        Object out = defaultTranscoder().decode(cd);

        assertThat(out).isInstanceOf(EVCacheValue.class);
        assertThat(out).isEqualTo(v);
    }

    // ---- 5. Non-EVCacheValue passthrough (arbitrary Java objects still use Java serde) ----

    @Test
    public void testNonEVCacheValuePassthrough() {
        EVCacheTranscoder t = binaryTranscoder(); // even with binary on, non-EVCacheValue stays Java
        ArrayList<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");

        CachedData cd = t.encode(list);
        Object out = t.decode(cd);

        assertThat(out).isEqualTo(list);
        // Routed through generic Java serialization, not the binary envelope.
        assertThat(cd.getFlags() & SERIALIZED).isNotZero();
        assertThat(cd.getData()[0]).isEqualTo(JAVA_STREAM_MAGIC_FIRST);
    }

    // ---- 6. Size win: binary smaller than Java for a representative item ----

    @Test
    public void testBinaryIsSmallerThanJava() throws Exception {
        EVCacheValue v = typical();
        int binaryLen = binaryTranscoder().encode(v).getData().length;
        int javaLen = javaSerializedLength(v);
        assertThat(binaryLen).isLessThan(javaLen);
    }

    // ---- 7. Malformed binary input is logged in EVCacheValueSerde and decodes to null ----
    //
    // EVCacheValueSerde.deserialize warn-logs the corruption (field + truncated hex) and returns
    // null. Callers see a cache miss rather than a thrown exception, matching the resilience
    // contract of BaseSerializingTranscoder.

    @Test
    public void testDecodeTruncatedBinaryReturnsNull() {
        byte[] full = binaryTranscoder().encode(typical()).getData();
        byte[] truncated = Arrays.copyOf(full, 3);
        CachedData cd = new CachedData(SERIALIZED, truncated, CachedData.MAX_SIZE);
        assertThat(defaultTranscoder().decode(cd)).isNull();
    }

    @Test
    public void testDecodeBinaryWithBogusKeyLengthReturnsNull() {
        // Magic + reserved + wildly oversized keyLength. Bounds check rejects.
        byte[] bytes = new byte[2 + Integer.BYTES];
        bytes[0] = EVCacheValueSerde.BINARY_SERDE_MAGIC_CONSTANT_BYTE;
        bytes[1] = 0x00;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(2, 0x7FFFFFFF);
        CachedData cd = new CachedData(SERIALIZED, bytes, CachedData.MAX_SIZE);
        assertThat(defaultTranscoder().decode(cd)).isNull();
    }

    @Test
    public void testDecodeBinaryWithNegativeKeyLengthReturnsNull() {
        byte[] bytes = new byte[2 + Integer.BYTES];
        bytes[0] = EVCacheValueSerde.BINARY_SERDE_MAGIC_CONSTANT_BYTE;
        bytes[1] = 0x00;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(2, -1);
        CachedData cd = new CachedData(SERIALIZED, bytes, CachedData.MAX_SIZE);
        assertThat(defaultTranscoder().decode(cd)).isNull();
    }

    // ---- 8. Forward compatibility trip-wire: pinned v0 payload must always decode ----
    //
    // If this test starts failing after a change to EVCacheValueSerde.deserialize(), someone
    // likely added a required field without the `buffer.hasRemaining()` guard. See the
    // "Additive optional" section of EVCacheValueSerde's Javadoc — a future reader must be
    // able to decode the v0 payload below (which an old writer would have produced) for as
    // long as items written by old writers can still be in any cache.
    //
    // The bytes here are intentionally FROZEN. Do not update them when adding fields.
    @Test
    public void testV0PayloadDecodesAsOptionalAdditiveFieldTripWire() {
        byte[] v0Bytes = {
            (byte) 0x0C,                                              // magic
            (byte) 0x00,                                              // reserved/version
            0x00, 0x00, 0x00, 0x01,                                   // keyLength = 1
            (byte) 'k',
            0x00, 0x00, 0x00, 0x01,                                   // valueLength = 1
            0x76,                                                     // value byte
            0x00, 0x00, 0x00, 0x01,                                   // flags = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C,           // ttl = 60
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,           // createTime = 42
        };
        CachedData cd = new CachedData(SERIALIZED, v0Bytes, CachedData.MAX_SIZE);
        Object out = defaultTranscoder().decode(cd);

        EVCacheValue expected = new EVCacheValue("k", new byte[] {0x76}, 1, 60L, 42L);
        assertThat(out)
                .as("Pinned v0 payload must decode cleanly. If it doesn't, a required field was "
                        + "likely added to deserialize() without the buffer.hasRemaining() guard. "
                        + "See EVCacheValueSerde Javadoc 'Additive optional'.")
                .isEqualTo(expected);
    }

    // ---- 9. Forward compatibility: newer-writer extension bytes past createTime ----
    //
    // A writer that adds new optional fields appends them after createTime. An older reader
    // (this one) reads its known fields, leaves the extension bytes unread, and returns the
    // EVCacheValue it does know how to decode — NOT a corruption event.

    @Test
    public void testDecodeBinaryWithFutureExtensionFieldsIsForwardCompat() {
        EVCacheValue v = typical();
        byte[] validBytes = binaryTranscoder().encode(v).getData();

        // Append 3 extension bytes past the end of the v0 envelope — what a future writer
        // would do. End of envelope is implicit at bytes.length, no header to update.
        byte[] withExtension = Arrays.copyOf(validBytes, validBytes.length + 3);

        CachedData cd = new CachedData(SERIALIZED, withExtension, CachedData.MAX_SIZE);
        Object out = defaultTranscoder().decode(cd);
        assertThat(out).isInstanceOf(EVCacheValue.class);
        assertThat(out).isEqualTo(v);
    }
}
