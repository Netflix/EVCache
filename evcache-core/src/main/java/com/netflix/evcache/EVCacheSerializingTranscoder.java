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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.netflix.evcache.config.EVCacheTranscoderProperties;
import com.netflix.evcache.config.EVCacheTranscoderProperties.CompressionAlgorithm;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.EVCacheValue;
import com.netflix.evcache.pool.EVCacheValueSerde;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Tag;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;

/**
 * Transcoder that serializes and compresses objects.
 */
public class EVCacheSerializingTranscoder extends BaseSerializingTranscoder implements
        Transcoder<Object> {

    private static final Logger log = LoggerFactory.getLogger(EVCacheSerializingTranscoder.class);

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

    public static final int DEFAULT_ZSTD_COMPRESSION_LEVEL = 3;

    private static final int ZSTD_MAGIC = 0xFD2FB528;

    private final TranscoderUtils tu = new TranscoderUtils(true);
    protected final String appName;
    protected EVCacheTranscoderProperties transcoderProperties;

    private final EnumMap<CompressionAlgorithm, DistributionSummary> compressionRatioSummaries;

    /**
     * Get a serializing transcoder with the default max data size.
     */
    public EVCacheSerializingTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    /**
     * Get a serializing transcoder that specifies the max data size. Builds a default
     * {@link EVCacheTranscoderProperties} bundle from
     * {@link EVCacheConfig#getInstance()} — subclasses/callers that want per-app
     * resolution should use {@link #EVCacheSerializingTranscoder(int, EVCacheTranscoderProperties)}.
     */
    public EVCacheSerializingTranscoder(int max) {
        this(max, new EVCacheTranscoderProperties(null, EVCacheConfig.getInstance().getPropertyRepository()));
    }

    /**
     * Get a serializing transcoder with the supplied transcoder-property bundle. The bundle is
     * exposed to subclasses via {@link #transcoderProperties} so downstream transcoders can consult
     * the same three-level (per-app → global → static default) resolution chain.
     */
    public EVCacheSerializingTranscoder(int max, EVCacheTranscoderProperties properties) {
        super(max);
        this.appName = properties.getAppName();
        this.transcoderProperties = properties;
        this.compressionRatioSummaries = buildCompressionRatioSummaries(appName);
    }

    private static EnumMap<CompressionAlgorithm, DistributionSummary> buildCompressionRatioSummaries(String appName) {
        EnumMap<CompressionAlgorithm, DistributionSummary> summaries = new EnumMap<>(CompressionAlgorithm.class);
        for (CompressionAlgorithm algo : CompressionAlgorithm.values()) {
            List<Tag> tagList = new ArrayList<>(2);
            tagList.add(new BasicTag(EVCacheMetricsFactory.COMPRESSION_TYPE, algo.name().toLowerCase()));
            if (appName != null && !appName.isEmpty()) {
                tagList.add(new BasicTag(EVCacheMetricsFactory.CACHE, appName));
            }
            summaries.put(algo, EVCacheMetricsFactory.getInstance().getDistributionSummary(EVCacheMetricsFactory.COMPRESSION_RATIO, tagList));
        }
        return summaries;
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
                default:
                    log.warn("Undecodeable with flags {}", Integer.toHexString(flags));
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
        } else {
            b = serialize(o);
            flags |= SERIALIZED;
        }
        assert b != null;
        if (b.length > compressionThreshold) {
            int originalLength = b.length;
            byte[] compressed = compress(b);
            if (compressed.length < originalLength) {
                log.trace("Compressed {} from {} to {}",
                        o.getClass().getName(), originalLength, compressed.length);
                b = compressed;
                flags |= COMPRESSED;
            } else {
                log.debug("Compression increased the size of {} from {} to {}",
                        o.getClass().getName(), originalLength, compressed.length);
            }
        }
        return new CachedData(flags, b, getMaxSize());
    }

    @Override
    protected byte[] serialize(Object o) {
        if (transcoderProperties.isBinarySerializationEnabled() && o instanceof EVCacheValue) {
            return EVCacheValueSerde.serialize((EVCacheValue) o);
        }
        return super.serialize(o);
    }

    @Override
    protected Object deserialize(byte[] in) {
        if (EVCacheValueSerde.isBinaryFormat(in)) {
            return EVCacheValueSerde.deserialize(in);
        }
        return super.deserialize(in);
    }

    @Override
    protected byte[] compress(byte[] in) {
        if (in == null) throw new NullPointerException("Can't compress null");

        CompressionAlgorithm compressionAlgorithm = transcoderProperties.getCompressionAlgorithmProperty().get();
        byte[] compressed;
        switch (compressionAlgorithm) {
            case ZSTD:
                int zstdLevel = transcoderProperties.getZstdCompressionLevelProperty().get();
                log.debug("algorithm: {}, level: {}, appName: {}", compressionAlgorithm, zstdLevel, appName);
                compressed = Zstd.compress(in, zstdLevel);
                break;
            case GZIP:
                log.debug("algorithm: {}, appName: {}", compressionAlgorithm, appName);
                compressed = super.compress(in);
                break;
            default:
                throw new IllegalArgumentException("Unsupported compression algorithm: " + compressionAlgorithm);
        }

        if (compressed != null) {
            long ratioPerCent = Math.round((double) compressed.length / in.length * 100.0);
            recordCompressionRatio(ratioPerCent, compressionAlgorithm);
        }

        return compressed;
    }

    @Override
    protected byte[] decompress(byte[] in) {
        if (in == null || in.length == 0) return in;
        if (isZstdCompressed(in)) return decompressZstd(in);
        return super.decompress(in);
    }

    private boolean isZstdCompressed(byte[] data) {
        if (data == null || data.length < 4) return false;
        int magic = ByteBuffer.wrap(data, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return magic == ZSTD_MAGIC;
    }

    private byte[] decompressZstd(byte[] in) {
        long originalSize = Zstd.getFrameContentSize(in);
        if (originalSize > Integer.MAX_VALUE) {
            throw new RuntimeException("Zstd decompressed size exceeds int range: " + originalSize);
        }
        if (originalSize > 0) {
            // Fast path: frame carries a content-size header (compress() above always does).
            return Zstd.decompress(in, (int) originalSize);
        }
        // Slow path: declared size is 0, unknown (-1), or invalid (-2) — stream-decode and let
        // ZstdInputStream surface any frame errors.
        log.warn("Zstd frame missing content-size header (getFrameContentSize={}); falling back to stream decode. appName={}", originalSize, appName);
        ZstdInputStream zis = null;
        try {
             zis = new ZstdInputStream(new ByteArrayInputStream(in));
            return readAll(zis);
        } catch (IOException e) {
            log.error("Error reading Zstd input stream", e);
            return null;
        } finally {
            try { if (zis != null) zis.close(); } catch (IOException ignored) {}
        }
    }

    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    private void recordCompressionRatio(long ratioPerCent, CompressionAlgorithm compressionAlgorithm) {
        DistributionSummary summary = compressionRatioSummaries.get(compressionAlgorithm);
        if (summary != null) {
            summary.record(ratioPerCent);
        }
    }
}
