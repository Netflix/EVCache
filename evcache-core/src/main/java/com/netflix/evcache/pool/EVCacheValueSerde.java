package com.netflix.evcache.pool;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Length-prefixed binary wire format for the {@link EVCacheValue} envelope. EVCache wraps a
 * value in an {@code EVCacheValue} when the canonical key has to be hashed (see
 * {@code EVCacheImpl.getEVCacheKey}) so the pre-hash key is preserved for collision detection.
 *
 * <pre>
 * [byte 0: magic 0x0C][byte 1: reserved/version 0x00]
 * [int keyLen][key UTF-8 bytes]
 * [int valLen][value bytes]
 * [int flags][long ttl][long createTime]
 * [... optional extension fields appended by newer writers ...]
 * </pre>
 *
 * <ul>
 *   <li><b>Magic {@code 0x0C}</b> disambiguates from Java {@code ObjectOutputStream} (starts
 *       {@code 0xAC 0xED}); callers route via {@link #isBinaryFormat(byte[])}.</li>
 *   <li><b>Reserved/version byte</b> is currently {@code 0x00}, read-and-ignored. Bump only
 *       for breaking changes (see Upgrades).</li>
 *   <li><b>End of envelope</b> is implicit at {@code bytes.length}. There is no declared body
 *       length on the wire; bytes past the last known field are treated as extension data for
 *       additive forward-compat (see Upgrades).</li>
 *   <li><b>Byte order:</b> big-endian / network, set explicitly on both sides.</li>
 *   <li><b>Error contract:</b> any corrupt/truncated input returns {@code null} after a WARN
 *       log identifying the failing field and a (truncated) hex dump of the bytes. Matches
 *       {@code BaseSerializingTranscoder}'s resilience contract — caller sees a cache miss.</li>
 * </ul>
 *
 * <h2>Upgrades</h2>
 *
 * <p><b>Additive optional (non-breaking).</b> Append a new field at the end of the envelope,
 * after {@code createTime}. Older readers stop after the known fields and never look at the
 * extension bytes. Newer readers MUST gate each added field with {@code buffer.hasRemaining()}
 * and supply a default when absent — they will encounter items written by old writers
 * (in cache until TTL expires) that don't contain the field. Only works when a graceful
 * default exists. A new <i>required</i> field has no acceptable default and is therefore
 * Breaking, not additive.
 *
 * <p><b>Breaking</b> (field reorder, type widen, semantic change, new required field):
 * rollout MUST be <i>reader-before-writer</i> — items written by an early writer would be
 * silently misparsed by lagging readers and survive until TTL.
 * <ol>
 *   <li>Ship a version-aware reader that branches on byte 1: {@code 0x00} stays on the current
 *       decoder, the new value routes to the new decoder, unknown values
 *       {@link #logCorruption(byte[], String)} and return {@code null}. Deploy to 100% of every
 *       consumer that calls {@link #deserialize} (clients, admin tools, cache warmers,
 *       replicators).</li>
 *   <li>Wait for the longer of (full reader rollout) and (max item TTL).</li>
 *   <li>Then ship the new writer gated by a per-app FastProperty so canary is possible.</li>
 *   <li>Never reuse a version byte value for a different layout.</li>
 *   <li>Keep the old decoder path indefinitely — items live until their TTL expires.</li>
 * </ol>
 */
public final class EVCacheValueSerde {

    private static final Logger log = LoggerFactory.getLogger(EVCacheValueSerde.class);

    static final byte BINARY_SERDE_MAGIC_CONSTANT_BYTE = 0x0C; // 12
    private static final byte RESERVED_VERSION_BYTE = 0x00;

    private static final int CORRUPT_PAYLOAD_LOG_LIMIT = 1024;

    private EVCacheValueSerde() {
        // Utility class; not instantiable.
    }

    /** True iff {@code bytes} starts with the binary envelope magic byte. */
    public static boolean isBinaryFormat(byte[] bytes) {
        return bytes != null && bytes.length > 0 && bytes[0] == BINARY_SERDE_MAGIC_CONSTANT_BYTE;
    }

    /**
     * Encode an {@link EVCacheValue} into its compact binary envelope. Key and value must be
     * non-null — the {@link com.netflix.evcache.EVCacheTranscoder} / {@code CachedData} pipeline
     * above already rejects nulls.
     */
    public static byte[] serialize(EVCacheValue v) {
        final byte[] keyBytes = v.getKey().getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytes = v.getValue();

        final int bufferSize =
                Byte.BYTES + Byte.BYTES                // magic + reserved/version
              + Integer.BYTES + keyBytes.length        // keyLen + key
              + Integer.BYTES + valueBytes.length      // valLen + value
              + Integer.BYTES                          // flags
              + Long.BYTES                             // ttl
              + Long.BYTES;                            // createTime
        final ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.BIG_ENDIAN);

        buffer.put(BINARY_SERDE_MAGIC_CONSTANT_BYTE);
        buffer.put(RESERVED_VERSION_BYTE);

        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
        buffer.putInt(v.getFlags());
        buffer.putLong(v.getTTL());
        buffer.putLong(v.getCreateTimeUTC());

        return buffer.array();
    }

    /**
     * Decode the binary envelope. Length prefixes are bounds-checked before allocation. A
     * truncated or malformed payload returns {@code null} after a WARN log identifying the
     * failing field. Bytes remaining past the known fields are not read — they're reserved for
     * additive extension fields appended by newer writers (see Upgrades).
     */
    public static EVCacheValue deserialize(byte[] bytes) {
        String field = "magic";
        try {
            final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

            final byte magic = buffer.get();
            if (BINARY_SERDE_MAGIC_CONSTANT_BYTE != magic) {
                logCorruption(bytes, "Invalid magic constant: " + magic);
                return null;
            }
            field = "reserved";
            buffer.get();

            field = "keyLength";
            final int keyLength = buffer.getInt();
            if (keyLength < 0 || keyLength > buffer.remaining()) {
                logCorruption(bytes, "Invalid keyLength: " + keyLength + ", remaining=" + buffer.remaining());
                return null;
            }
            field = "key";
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            field = "valueLength";
            final int valueLength = buffer.getInt();
            if (valueLength < 0 || valueLength > buffer.remaining()) {
                logCorruption(bytes, "Invalid valueLength: " + valueLength + ", remaining=" + buffer.remaining());
                return null;
            }
            field = "value";
            final byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);

            field = "flags";
            final int flags = buffer.getInt();
            field = "ttl";
            final long ttl = buffer.getLong();
            field = "createTime";
            final long createTime = buffer.getLong();

            // Any remaining bytes are forward-compat extension fields a newer writer appended;
            // an older reader (this one) leaves them unread.

            return new EVCacheValue(key, valueBytes, flags, ttl, createTime);
        } catch (BufferUnderflowException e) {
            logCorruption(bytes, "BufferUnderflow at field '" + field + "'");
            return null;
        } catch (Exception e) {
            log.warn("Uncaught exception decoding {} bytes of EVCacheValue binary envelope at field '{}'",
                    bytes.length, field, e);
            return null;
        }
    }

    /**
     * Warn-log a corruption event with byte length, failure reason, and a (truncated) hex dump.
     * No Throwable — corruption is expected/recoverable at WARN level; a stack trace would be
     * noise. Hex capped at {@value #CORRUPT_PAYLOAD_LOG_LIMIT} bytes.
     */
    private static void logCorruption(byte[] bytes, String error) {
        log.warn("Failed to deserialize {} bytes of EVCacheValue binary envelope, error={}, payload hex: {}",
                bytes.length, error, toHex(bytes, CORRUPT_PAYLOAD_LOG_LIMIT));
    }

    private static String toHex(byte[] bytes, int maxBytes) {
        if (bytes == null) {
            return "null";
        }
        if (bytes.length <= maxBytes) {
            return Hex.encodeHexString(bytes);
        }
        return Hex.encodeHexString(Arrays.copyOf(bytes, maxBytes))
                + "...(truncated, total=" + bytes.length + " bytes)";
    }
}
