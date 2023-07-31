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
import com.github.luben.zstd.ZstdOutputStream;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.evcache.util.Compressor;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.BaseSerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.transcoders.TranscoderUtils;
import net.spy.memcached.util.StringUtils;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


/**
 * Transcoder that serializes and compresses objects.
 */
public class EVCacheSerializingTranscoder extends BaseSerializingTranscoder implements
        Transcoder<Object> {

    // General flags
    static final int SERIALIZED = 1;
    static final int COMPRESSED = 2;
    static final int ZSTD_COMPRESSION = 3;
    static final int LZ4_COMPRESSION = 4;
    static final int SNAPPY_COMPRESSION = 5;
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

    static final String COMPRESSION = "COMPRESSION_METRIC";

    private final TranscoderUtils tu = new TranscoderUtils(true);
    private final Compressor compressor;
    private final String compressionAlgo;
    private final Integer compressionLevel;

    private Timer timer;

    /**
     * Get a serializing transcoder with the default max data size.
     */
    public EVCacheSerializingTranscoder(String appName) {
        this(appName, CachedData.MAX_SIZE);
    }

    /**
     * Get a serializing transcoder that specifies the max data size.
     */
    public EVCacheSerializingTranscoder(String appName, int max) {
        super(max);
        compressor = new Compressor();
        this.compressionAlgo = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".compression.algo", String.class).orElse("gzip").get();
        this.compressionLevel = EVCacheConfig.getInstance().getPropertyRepository().get(appName + ".compression.level", Integer.class).orElse(3).get();
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
            try {
                data = compressor.decompress(d.getData(), compressionAlgo);
            } catch (IOException e) {
                getLogger().error("throwing exception in decoding due to compression {}", e);
                throw new RuntimeException(e);
            }
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
            if (StringUtils.isJsonObject((String) o)) {
                return new CachedData(flags, b, getMaxSize());
            }
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
            byte[] compressed;
            try {
                compressed = compressor.compress(b, compressionAlgo, compressionLevel);
            } catch (IOException e) {
                getLogger().error("throwing exception in encoding due to compression {}", e);
                throw new RuntimeException(e);
            }
            if (compressed.length < b.length) {
                getLogger().debug("Compressed %s from %d to %d",
                        o.getClass().getName(), b.length, compressed.length);
                b = compressed;
                flags |= COMPRESSED;
                switch (compressionAlgo) {
                    case "zstd":
                        flags |= ZSTD_COMPRESSION;
                        break;
                    case "lz4":
                        flags |= LZ4_COMPRESSION;
                        break;
                    case "snappy":
                        flags |= SNAPPY_COMPRESSION;
                        break;
                }
            } else {
                getLogger().info("Compression increased the size of %s from %d to %d",
                        o.getClass().getName(), b.length, compressed.length);
            }

            long compression_ratio = Math.round((double) compressed.length / b.length * 100);
            updateTimerWithCompressionRatio(compression_ratio);
        }
        return new CachedData(flags, b, getMaxSize());
    }

    private void updateTimerWithCompressionRatio(long ratio_percentage) {
        if(timer == null) {
            final List<Tag> tagList = new ArrayList<Tag>(1);
            tagList.add(new BasicTag(EVCacheMetricsFactory.COMPRESSION_TYPE, compressionAlgo));
            timer = EVCacheMetricsFactory.getInstance().getPercentileTimer(EVCacheMetricsFactory.COMPRESSION_RATIO, tagList, Duration.ofMillis(100));
        };
        timer.record(ratio_percentage, TimeUnit.MILLISECONDS);
    }

}
