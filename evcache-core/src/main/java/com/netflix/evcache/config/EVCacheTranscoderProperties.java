package com.netflix.evcache.config;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;

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
 *   Static properties are snapshotted as primitive fields for fast access. Dynamic fields
 *   are exposed as {@link Property} accessors so callers see live FP updates on every
 *   {@code .get()}.
 *
 */
public final class EVCacheTranscoderProperties {

    public static final boolean DEFAULT_BINARY_SERIALIZATION_ENABLED = false;
    public static final int DEFAULT_MAX_DATA_SIZE_BYTES = 20 * 1024 * 1024;
    public static final int DEFAULT_COMPRESSION_THRESHOLD_BYTES = 120;

    public enum Key {
        BINARY_SERIALIZATION_ENABLED("binary.serialization.enabled", "default.evcache.binary.serialization.enabled"),
        MAX_DATA_SIZE_BYTES("max.data.size", "default.evcache.max.data.size"),
        COMPRESSION_THRESHOLD_BYTES("compression.threshold", "default.evcache.compression.threshold");

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
        this.maxDataSizeBytes = getProperty(appName, propertyRepository,
                Key.MAX_DATA_SIZE_BYTES, Integer.class, DEFAULT_MAX_DATA_SIZE_BYTES).get();
        this.compressionThresholdBytes = getProperty(appName, propertyRepository,
                Key.COMPRESSION_THRESHOLD_BYTES, Integer.class, DEFAULT_COMPRESSION_THRESHOLD_BYTES).get();
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
