/**
 * Copyright (C) 2006-2009 Dustin Sallings
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.netflix.evcache;

import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;


/**
 * Transcoder that serializes and compresses objects.
 */
public class EVCacheSerializingTranscoder extends BaseSerializingTranscoder implements
        Transcoder<Object> {

    // General flags
    static final int SERIALIZED = 1;
    static final int COMPRESSED = 2;

    // Special flags for specially handled types.
    private static final int SPECIAL_MASK = 0xff00;
    static final int SPECIAL_BOOLEAN = (1 << 8);
    static final int SPECIAL_INT = (2 << 8);
    static final int SPECIAL_LONG = (3 << 8);
    static final int SPECIAL_DATE = (4 << 8);
    static final int SPECIAL_BYTE = (5 << 8);
    static final int SPECIAL_FLOAT = (6 << 8);
    static final int SPECIAL_DOUBLE = (7 << 8);
    static final int SPECIAL_BYTEARRAY = (8 << 8);
    static final int SPECIAL_EVCACHEVALUE = (9 << 8);

    // EVCacheValue serialization version for schema evolution
    // When adding new fields to EVCacheValue:
    // 1. Define EVCACHEVALUE_VERSION_2 = 2
    // 2. Update serializeEvCacheValue() to write version 2 format
    // 3. Add deserializeEvCacheValueV2() method to read new format
    // 4. Update deserializeEvCacheValue() switch to handle version 2
    // 5. Keep deserializeEvCacheValueV1() for backwards compatibility
    private static final byte EVCACHEVALUE_VERSION_1 = 1;

    static final String COMPRESSION = "COMPRESSION_METRIC";

    private final TranscoderUtils tu = new TranscoderUtils(true);
    private Timer timer;
    private final boolean useCompactEvCacheValueSerialization;

    /**
     * Get a serializing transcoder with the default max data size.
     */
    public EVCacheSerializingTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    /**
     * Get a serializing transcoder that specifies the max data size.
     */
    public EVCacheSerializingTranscoder(int max) {
        this(max, false);
    }

    /**
     * Get a serializing transcoder that specifies the max data size and EVCacheValue serialization mode.
     * @param max Maximum data size
     * @param useCompactEvCacheValueSerialization When false, uses Java object serialization for EVCacheValue (old format, backwards compatible).
     *                                            When true, uses compact binary format for EVCacheValue (new format, more efficient).
     */
    public EVCacheSerializingTranscoder(int max, boolean useCompactEvCacheValueSerialization) {
        super(max);
        this.useCompactEvCacheValueSerialization = useCompactEvCacheValueSerialization;
    }

    @Override
    public boolean asyncDecode(CachedData d) {
        if ((d.getFlags() & COMPRESSED) != 0 || (d.getFlags() & SERIALIZED) != 0) {
            return true;
        }
        return super.asyncDecode(d);
    }

    /*
     * (non-Javadoc)
     *
     * @see net.spy.memcached.Transcoder#decode(net.spy.memcached.CachedData)
     */
    public Object decode(CachedData d) {
        byte[] data = d.getData();
        Object rv = null;
        if ((d.getFlags() & COMPRESSED) != 0) {
            data = decompress(d.getData());
        }
        int flags = d.getFlags() & SPECIAL_MASK;
        if ((d.getFlags() & SERIALIZED) != 0 && data != null) {
            rv = deserialize(data);
        } else if (flags != 0 && data != null) {
            switch (flags) {
                case SPECIAL_BOOLEAN:
                    rv = Boolean.valueOf(tu.decodeBoolean(data));
                    break;
                case SPECIAL_INT:
                    rv = Integer.valueOf(tu.decodeInt(data));
                    break;
                case SPECIAL_LONG:
                    rv = Long.valueOf(tu.decodeLong(data));
                    break;
                case SPECIAL_DATE:
                    rv = new Date(tu.decodeLong(data));
                    break;
                case SPECIAL_BYTE:
                    rv = Byte.valueOf(tu.decodeByte(data));
                    break;
                case SPECIAL_FLOAT:
                    rv = new Float(Float.intBitsToFloat(tu.decodeInt(data)));
                    break;
                case SPECIAL_DOUBLE:
                    rv = new Double(Double.longBitsToDouble(tu.decodeLong(data)));
                    break;
                case SPECIAL_BYTEARRAY:
                    rv = data;
                    break;
                // TODO: Additional Backwards Compatibility Risks:
                //
                //  1. During a rolling deployment where some nodes have new code and some have old:
                //  - New code writes EVCacheValue → uses SPECIAL_EVCACHEVALUE flag
                //  - Old code reads it → FAILS (returns null)
                //  - This creates runtime failures during the deployment window
                //  - Even worse in a multi-region deployment with staggered rollouts
                //
                //  2. If you deploy new code then need to rollback:
                //  - New code has written SPECIAL_EVCACHEVALUE entries to cache
                //  - After rollback, old code can't read these entries
                //  - Cache poisoning: cached data becomes unreadable until TTL expires
                //  - May require manual cache invalidation
                //
                case SPECIAL_EVCACHEVALUE:
                    rv = deserializeEvCacheValue(data);
                    break;
                default:
                    getLogger().warn("Undecodeable with flags %x", flags);
            }
        } else {
            rv = decodeString(data);
        }
        return rv;
    }

    /*
     * (non-Javadoc)
     *
     * @see net.spy.memcached.Transcoder#encode(java.lang.Object)
     */
    public CachedData encode(Object o) {
        byte[] b = null;
        int flags = 0;
        if (o instanceof String) {
            b = encodeString((String) o);
        } else if (o instanceof Long) {
            b = tu.encodeLong((Long) o);
            flags |= SPECIAL_LONG;
        } else if (o instanceof Integer) {
            b = tu.encodeInt((Integer) o);
            flags |= SPECIAL_INT;
        } else if (o instanceof Boolean) {
            b = tu.encodeBoolean((Boolean) o);
            flags |= SPECIAL_BOOLEAN;
        } else if (o instanceof Date) {
            b = tu.encodeLong(((Date) o).getTime());
            flags |= SPECIAL_DATE;
        } else if (o instanceof Byte) {
            b = tu.encodeByte((Byte) o);
            flags |= SPECIAL_BYTE;
        } else if (o instanceof Float) {
            b = tu.encodeInt(Float.floatToRawIntBits((Float) o));
            flags |= SPECIAL_FLOAT;
        } else if (o instanceof Double) {
            b = tu.encodeLong(Double.doubleToRawLongBits((Double) o));
            flags |= SPECIAL_DOUBLE;
        } else if (o instanceof byte[]) {
            b = (byte[]) o;
            flags |= SPECIAL_BYTEARRAY;
        } else if (useCompactEvCacheValueSerialization && o instanceof EVCacheValue) {
            b = serializeEvCacheValue((EVCacheValue) o);
            flags |= SPECIAL_EVCACHEVALUE;
        } else {
            b = serialize(o);
            flags |= SERIALIZED;
        }
        assert b != null;
        if (b.length > compressionThreshold) {
            byte[] compressed = compress(b);
            if (compressed.length < b.length) {
                getLogger().trace("Compressed %s from %d to %d",
                        o.getClass().getName(), b.length, compressed.length);
                b = compressed;
                flags |= COMPRESSED;
            } else {
                getLogger().debug("Compression increased the size of %s from %d to %d",
                        o.getClass().getName(), b.length, compressed.length);
            }

            long compression_ratio = Math.round((double) compressed.length / b.length * 100);
            updateTimerWithCompressionRatio(compression_ratio);
        }
        return new CachedData(flags, b, getMaxSize());
    }

    /**
     * Serializes EVCacheValue to compact binary format with versioning for schema evolution.
     *
     * Current format (Version 1):
     *   byte     version (1)
     *   int      keyLen (-1 if null)
     *   byte[]   key (UTF-8 encoded, omitted if null)
     *   int      valueLen (-1 if null)
     *   byte[]   value (omitted if null)
     *   int      flags
     *   long     ttl
     *   long     createTimeUTC
     *
     * Future evolution example (Version 2):
     *   To add a new field (e.g., lastAccessTimeUTC):
     *   - Define EVCACHEVALUE_VERSION_2 = 2
     *   - Update this method to write V2 format with new field at end
     *   - Add deserializeEvCacheValueV2() to read V2 format
     *   - Keep deserializeEvCacheValueV1() unchanged for backwards compatibility
     *   - Update deserializeEvCacheValue() to dispatch based on version
     */
    private byte[] serializeEvCacheValue(EVCacheValue evCacheValue) {
        String key = evCacheValue.getKey();
        byte[] value = evCacheValue.getValue();
        byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;

        int keyLen = keyBytes != null ? keyBytes.length : -1;
        int valueLen = value != null ? value.length : -1;

        int size = 1 + 4 + (keyBytes != null ? keyBytes.length : 0)
                 + 4 + (value != null ? value.length : 0)
                 + 4 + 8 + 8;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put(EVCACHEVALUE_VERSION_1);
        buf.putInt(keyLen);
        if (keyBytes != null) {
            buf.put(keyBytes);
        }
        buf.putInt(valueLen);
        if (value != null) {
            buf.put(value);
        }
        buf.putInt(evCacheValue.getFlags());
        buf.putLong(evCacheValue.getTTL());
        buf.putLong(evCacheValue.getCreateTimeUTC());
        return buf.array();
    }

    private EVCacheValue deserializeEvCacheValue(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        byte version = buf.get();

        if (version == EVCACHEVALUE_VERSION_1) {
            return deserializeEvCacheValueV1(buf);
        } else {
            // Future versions would be handled here
            // For now, we can try to be forward compatible by reading V1 format
            getLogger().warn("Unknown EVCacheValue version: %d, attempting V1 deserialization", version);
            return deserializeEvCacheValueV1(buf);
        }
    }

    private EVCacheValue deserializeEvCacheValueV1(ByteBuffer buf) {
        int keyLength = buf.getInt();
        String key = null;
        if (keyLength >= 0) {
            byte[] keyBytes = new byte[keyLength];
            buf.get(keyBytes);
            key = new String(keyBytes, StandardCharsets.UTF_8);
        }
        int valueLength = buf.getInt();
        byte[] value = null;
        if (valueLength >= 0) {
            value = new byte[valueLength];
            buf.get(value);
        }
        int flags = buf.getInt();
        long ttl = buf.getLong();
        long createTimeUTC = buf.getLong();
        return new EVCacheValue(key, value, flags, ttl, createTimeUTC);
    }

    private void updateTimerWithCompressionRatio(long ratio_percentage) {
        if(timer == null) {
            final List<Tag> tagList = new ArrayList<Tag>(1);
            tagList.add(new BasicTag(EVCacheMetricsFactory.COMPRESSION_TYPE, "gzip"));
            timer = EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.COMPRESSION_RATIO, tagList, Duration.ofMillis(100));
        };

        timer.record(ratio_percentage, TimeUnit.MILLISECONDS);
    }

}
