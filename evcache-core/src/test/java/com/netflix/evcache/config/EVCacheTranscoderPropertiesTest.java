package com.netflix.evcache.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;

import org.testng.annotations.Test;

/**
 * Three-level resolution tests for each key in {@link EVCacheTranscoderProperties}:
 * per-app override → global default → static default. Property keys are asserted as
 * string literals so a rename of a {@link EVCacheTranscoderProperties.Key} entry
 * trips these tests loudly.
 */
public class EVCacheTranscoderPropertiesTest {

    private static final String APP = "MYAPP";
    private static final String BINARY_PER_APP_KEY = "MYAPP.binary.serialization.enabled";
    private static final String BINARY_GLOBAL_KEY = "default.evcache.binary.serialization.enabled";
    private static final String MAX_DATA_SIZE_PER_APP_KEY = "MYAPP.max.data.size";
    private static final String MAX_DATA_SIZE_GLOBAL_KEY = "default.evcache.max.data.size";
    private static final String COMPRESSION_PER_APP_KEY = "MYAPP.compression.threshold";
    private static final String COMPRESSION_GLOBAL_KEY = "default.evcache.compression.threshold";

    private static PropertyRepository repo(DefaultSettableConfig cfg) {
        return DefaultPropertyFactory.from(cfg);
    }

    // ---- BINARY_SERIALIZATION_ENABLED ----

    @Test
    public void binarySerialization_perAppOverrideWins() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(BINARY_PER_APP_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isTrue();
    }

    @Test
    public void binarySerialization_globalFallbackWhenPerAppUnset() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(BINARY_GLOBAL_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isTrue();
    }

    @Test
    public void binarySerialization_staticDefaultWhenBothUnset() {
        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(new DefaultSettableConfig()));
        assertThat(props.isBinarySerializationEnabled()).isFalse();
    }

    @Test
    public void binarySerialization_perAppBeatsGlobal() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(BINARY_PER_APP_KEY, "false");
        cfg.setProperty(BINARY_GLOBAL_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isFalse();
    }

    @Test
    public void binarySerialization_nullAppNameUsesGlobalKey() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(BINARY_GLOBAL_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(null, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isTrue();
    }

    // ---- MAX_DATA_SIZE ----

    @Test
    public void maxDataSize_perAppOverrideWins() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(MAX_DATA_SIZE_PER_APP_KEY, "12345");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getMaxDataSizeBytes()).isEqualTo(12345);
    }

    @Test
    public void maxDataSize_globalFallbackWhenPerAppUnset() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(MAX_DATA_SIZE_GLOBAL_KEY, "12345");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getMaxDataSizeBytes()).isEqualTo(12345);
    }

    @Test
    public void maxDataSize_staticDefaultWhenBothUnset() {
        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(new DefaultSettableConfig()));
        assertThat(props.getMaxDataSizeBytes()).isEqualTo(EVCacheTranscoderProperties.DEFAULT_MAX_DATA_SIZE_BYTES);
    }

    @Test
    public void maxDataSize_perAppBeatsGlobal() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(MAX_DATA_SIZE_PER_APP_KEY, "111");
        cfg.setProperty(MAX_DATA_SIZE_GLOBAL_KEY, "222");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getMaxDataSizeBytes()).isEqualTo(111);
    }

    @Test
    public void maxDataSize_nullAppNameUsesGlobalKey() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(MAX_DATA_SIZE_GLOBAL_KEY, "12345");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(null, repo(cfg));
        assertThat(props.getMaxDataSizeBytes()).isEqualTo(12345);
    }

    // ---- COMPRESSION_THRESHOLD ----

    @Test
    public void compressionThreshold_perAppOverrideWins() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(COMPRESSION_PER_APP_KEY, "512");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getCompressionThresholdBytes()).isEqualTo(512);
    }

    @Test
    public void compressionThreshold_globalFallbackWhenPerAppUnset() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(COMPRESSION_GLOBAL_KEY, "512");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getCompressionThresholdBytes()).isEqualTo(512);
    }

    @Test
    public void compressionThreshold_staticDefaultWhenBothUnset() {
        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(new DefaultSettableConfig()));
        assertThat(props.getCompressionThresholdBytes()).isEqualTo(EVCacheTranscoderProperties.DEFAULT_COMPRESSION_THRESHOLD_BYTES);
    }

    @Test
    public void compressionThreshold_perAppBeatsGlobal() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(COMPRESSION_PER_APP_KEY, "111");
        cfg.setProperty(COMPRESSION_GLOBAL_KEY, "222");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.getCompressionThresholdBytes()).isEqualTo(111);
    }

    @Test
    public void compressionThreshold_nullAppNameUsesGlobalKey() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(COMPRESSION_GLOBAL_KEY, "512");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(null, repo(cfg));
        assertThat(props.getCompressionThresholdBytes()).isEqualTo(512);
    }

    // ---- appName ----

    @Test
    public void appName_isExposedForSubclassLookups() {
        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(new DefaultSettableConfig()));
        assertThat(props.getAppName()).isEqualTo(APP);
    }

    @Test
    public void appName_nullPropagates() {
        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(null, repo(new DefaultSettableConfig()));
        assertThat(props.getAppName()).isNull();
    }
}
