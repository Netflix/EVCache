package com.netflix.evcache.config;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;

/**
 * Typed access to the FastProperties that govern {@link com.netflix.evcache.EVCacheTranscoder}
 * behavior. Today the bundle holds a single entry — {@link Key#USE_BINARY_SERIALIZATION} — but
 * the {@link Key} enum is the extension point: additional transcoder properties land here and
 * inherit the same resolution chain without touching call sites.
 *
 * <p>Every property resolves at construction through:
 *
 * <ol>
 *   <li><b>Per-app override:</b> {@code <appName>.<appKeySuffix>}</li>
 *   <li><b>Global default:</b> {@code <globalKey>}</li>
 *   <li><b>Static default:</b> the value baked into this class</li>
 * </ol>
 *
 * <p>Properties are read once at construction and cached as primitives. A future field that
 * needs runtime mutability can skip the cached primitive and call
 * {@link #getProperty(Key, Class, Object)} on each access — the three-level resolution
 * applies to dynamic reads too.
 */
public class EVCacheTranscoderProperties {


    public enum Key {
        USE_BINARY_SERIALIZATION("binary.serialization.enabled", "default.evcache.binary.serialization.enabled");

        final String appKeySuffix;
        final String globalKey;

        Key(String appKeySuffix, String globalKey) {
            this.appKeySuffix = appKeySuffix;
            this.globalKey = globalKey;
        }
    }

    private static final boolean DEFAULT_BINARY_SERIALIZATION_ENABLED = false;

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
                Key.USE_BINARY_SERIALIZATION, Boolean.class, DEFAULT_BINARY_SERIALIZATION_ENABLED).get();
    }

    public boolean isBinarySerializationEnabled() {
        return binarySerializationEnabled;
    }

    /**
     * Read a property dynamically (re-evaluates on every call), with the same per-app -> global ->
     * static-default chain used for the cached fields above.
     */
    public <T> T getProperty(Key key, Class<T> type, T defaultValue) {
        return getProperty(appName, propertyRepository, key, type, defaultValue).get();
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
