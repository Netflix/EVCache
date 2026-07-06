package com.netflix.evcache.config;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import net.spy.memcached.compat.SpyObject;

/**
 * Properties related to {@link com.netflix.evcache.EVCacheTranscoder}
 * behavior.
 *
 * <p>Every property resolves at construction through:
 *
 * <ol>
 *   <li><b>Per-app override:</b> {@code <appName>.<appKeySuffix>}</li>
 *   <li><b>Global default:</b> {@code <globalKey>}</li>
 *   <li><b>Static default:</b> default value supplied as an argument</li>
 * </ol>
 *
 * <p>
 *   Static properties should be cached as a field for fast access.
 *   Dynamic properties are resolved through
 *   {@link #getProperty(String, PropertyRepository, Key, Class, Object)}.
 *
 */
public final class EVCacheTranscoderProperties extends SpyObject {

    public static final boolean DEFAULT_BINARY_SERIALIZATION_ENABLED = false;
    public static final int DEFAULT_MAX_DATA_SIZE_BYTES = 20 * 1024 * 1024;
    public static final int DEFAULT_COMPRESSION_THRESHOLD_BYTES = 120;
    public static final CompressionAlgorithm DEFAULT_COMPRESSION_ALGORITHM = CompressionAlgorithm.GZIP;
    public static final int DEFAULT_COMPRESSION_ZSTD_LEVEL = 3;

    public enum CompressionAlgorithm { GZIP, ZSTD }

    public enum Key {
        BINARY_SERIALIZATION_ENABLED("binary.serialization.enabled", "default.evcache.binary.serialization.enabled"),
        MAX_DATA_SIZE_BYTES("max.data.size", "default.evcache.max.data.size"),
        COMPRESSION_THRESHOLD_BYTES("compression.threshold", "default.evcache.compression.threshold"),
        COMPRESSION_ALGORITHM("compression.algorithm", "default.evcache.compression.algorithm"),
        COMPRESSION_ZSTD_LEVEL("compression.zstd.level", "default.evcache.compression.zstd.level");

        final String appKeySuffix;
        final String globalKey;

        Key(String appKeySuffix, String globalKey) {
            this.appKeySuffix = appKeySuffix;
            this.globalKey = globalKey;
        }
    }

    private final String appName;

    private final boolean binarySerializationEnabled;
    private final int maxDataSizeBytes;
    private final int compressionThresholdBytes;
    private final Property<CompressionAlgorithm> compressionAlgorithmProperty;
    private final Property<Integer> zstdCompressionLevelProperty;

    /**
     * Construct the bundle and snapshot every property via the three-level resolution chain.
     *
     * @param appName            the EVCache app name used as the per-app override prefix
     *                           (e.g. {@code "EVCACHE_FOO"}). When {@code null} or empty the
     *                           per-app step is skipped and resolution starts at the global
     *                           key — useful for the no-app transcoder constructors and for
     *                           callers that only want fleet-wide defaults.
     * @param propertyRepository the Archaius2 PropertyRepository to resolve against. Never null;
     *                           pass {@code EVCacheConfig.getInstance().getPropertyRepository()}
     *                           for the production wiring.
     */
    public EVCacheTranscoderProperties(String appName, PropertyRepository propertyRepository) {
        this.appName = appName;

        this.binarySerializationEnabled = getProperty(appName, propertyRepository,
                Key.BINARY_SERIALIZATION_ENABLED, Boolean.class, DEFAULT_BINARY_SERIALIZATION_ENABLED).get();
        this.maxDataSizeBytes = getProperty(appName, propertyRepository, Key.MAX_DATA_SIZE_BYTES, Integer.class, DEFAULT_MAX_DATA_SIZE_BYTES).get();
        this.compressionThresholdBytes = getProperty(appName, propertyRepository, Key.COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES).get();
        this.compressionAlgorithmProperty = getProperty(appName, propertyRepository,
                Key.COMPRESSION_ALGORITHM, String.class, DEFAULT_COMPRESSION_ALGORITHM.name())
                .map(this::parseCompressionAlgorithm);
        this.zstdCompressionLevelProperty = getProperty(appName, propertyRepository,
                Key.COMPRESSION_ZSTD_LEVEL, Integer.class, DEFAULT_COMPRESSION_ZSTD_LEVEL);
    }

    public String getAppName() {
        return appName;
    }

    public boolean isBinarySerializationEnabled() {
        return binarySerializationEnabled;
    }

    public int getMaxDataSizeBytes() {
        return maxDataSizeBytes;
    }

    public int getCompressionThresholdBytes() {
        return compressionThresholdBytes;
    }

    /**
     * Live-updating {@link Property} handle for the transcoder compression algorithm. Calling
     * {@code .get()} returns the current {@link CompressionAlgorithm} value; underlying storage
     * is a String property (case-insensitive) so ops can set the FP as {@code "gzip"} or
     * {@code "ZSTD"} interchangeably. Handle is resolved once at bundle construction and
     * shared across callers — {@code .get()} on it always observes the latest FP value.
     */
    public Property<CompressionAlgorithm> getCompressionAlgorithmProperty() {
        return compressionAlgorithmProperty;
    }

    /**
     * Live-updating {@link Property} handle for the zstd compression level. Resolved once at
     * bundle construction.
     */
    public Property<Integer> getZstdCompressionLevelProperty() {
        return zstdCompressionLevelProperty;
    }

    /**
     * Parse an FP algorithm string (case-insensitive) into a {@link CompressionAlgorithm}. An
     * unrecognized value falls back to {@link #DEFAULT_COMPRESSION_ALGORITHM} rather than
     * propagating a {@code null} (which would NPE the compression switch at encode time) — a
     * typo'd fast property degrades to the default instead of taking down writes.
     */
    private CompressionAlgorithm parseCompressionAlgorithm(String value) {
        try {
            return CompressionAlgorithm.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException | NullPointerException e) {
            getLogger().warn("Unrecognized compression algorithm '{}'; falling back to {}", value, DEFAULT_COMPRESSION_ALGORITHM);
            return DEFAULT_COMPRESSION_ALGORITHM;
        }
    }

    private static <T> Property<T> getProperty(String appName, PropertyRepository propertyRepository,
                                           Key key, Class<T> type, T defaultValue) {
        if (appName == null || appName.isEmpty()) {
            return propertyRepository.get(key.globalKey, type).orElse(defaultValue);
        }
        return propertyRepository.get(appName + "." + key.appKeySuffix, type)
                .orElseGet(key.globalKey)
                .orElse(defaultValue);
    }
}
