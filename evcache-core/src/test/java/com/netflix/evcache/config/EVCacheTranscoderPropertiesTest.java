package com.netflix.evcache.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.DefaultSettableConfig;

import org.testng.annotations.Test;

/**
 * Three-level resolution tests for {@link EVCacheTranscoderProperties#isBinarySerializationEnabled()}:
 * per-app override → global default → static default.
 */
public class EVCacheTranscoderPropertiesTest {

    private static final String APP = "MYAPP";
    private static final String PER_APP_KEY = "MYAPP.binary.serialization.enabled";
    private static final String GLOBAL_KEY = "default.evcache.binary.serialization.enabled";

    private static PropertyRepository repo(DefaultSettableConfig cfg) {
        return DefaultPropertyFactory.from(cfg);
    }

    @Test
    public void binarySerialization_perAppOverrideWins() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(PER_APP_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isTrue();
    }

    @Test
    public void binarySerialization_globalFallbackWhenPerAppUnset() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(GLOBAL_KEY, "true");

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
        cfg.setProperty(PER_APP_KEY, "false");
        cfg.setProperty(GLOBAL_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(APP, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isFalse();
    }

    @Test
    public void binarySerialization_nullAppNameUsesGlobalKey() {
        DefaultSettableConfig cfg = new DefaultSettableConfig();
        cfg.setProperty(GLOBAL_KEY, "true");

        EVCacheTranscoderProperties props = new EVCacheTranscoderProperties(null, repo(cfg));
        assertThat(props.isBinarySerializationEnabled()).isTrue();
    }
}
