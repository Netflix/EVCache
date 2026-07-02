package com.netflix.evcache.config;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;

import static com.netflix.evcache.config.EVCacheTranscoderProperties.Key.BINARY_SERIALIZATION_ENABLED;

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
 *   Dynamic properties get be accessed {@link #getProperty(Key, Class, Object)}
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
    private final PropertyRepository propertyRepository;

    private final boolean binarySerializationEnabled;

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
        this.propertyRepository = propertyRepository;
        this.binarySerializationEnabled = getProperty(appName, propertyRepository,
                BINARY_SERIALIZATION_ENABLED, Boolean.class, DEFAULT_BINARY_SERIALIZATION_ENABLED).get();
    }

    public boolean isBinarySerializationEnabled() {
        return binarySerializationEnabled;
    }

    /**
     * Resolve the Archaius {@link Property} handle for the given key. Callers should hold the
     * handle (as a final field, typically) and invoke {@link Property#get()} when they need the
     * current value; every {@code .get()} re-reads through the same per-app → global → static-default
     * chain, so live FP updates propagate without re-resolving. Returning the handle rather than
     * the resolved value makes it obvious that this is a dynamic property, not a static snapshot.
     */
    public <T> Property<T> getProperty(Key key, Class<T> type, T defaultValue) {
        return getProperty(appName, propertyRepository, key, type, defaultValue);
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
