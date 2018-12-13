package com.netflix.evcache;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class EVCacheBuilderTests {
  private static final Logger logger = LoggerFactory.getLogger(EVCacheBuilderTests.class);

  @Test
  public void shouldNotSetTranscoderWithoutObjectProvider() {
    final EVCache.Builder builder = EVCache.Builder.forApp("EVCACHE_BUILDER_TEST");

    builder
        .withConfigurationProperties(new EVCacheClientPoolConfigurationProperties() {
          @Override
          public String getTranscoder() {
            return "com.netflix.evcache.TranscoderStub";
          }
        })
        .setDefaultTTL(3600)
        .build();

    assertThat(builder.getTranscoder())
        .isNull();
  }

  @Test
  public void shouldSetTranscoderGivenObjectProvider() {
    final EVCache.Builder builder = EVCache.Builder.forApp("EVCACHE_BUILDER_TEST");

    builder
        .withObjectProvider((fqpn, errorMessage) -> {
          try {
            return Class.forName(fqpn).newInstance();
          } catch (ClassNotFoundException|IllegalAccessException|InstantiationException e) {
            logger.warn(errorMessage);

            return null;
          }
        })
        .withConfigurationProperties(new EVCacheClientPoolConfigurationProperties() {
          @Override
          public String getTranscoder() {
            return "com.netflix.evcache.TranscoderStub";
          }
        })
        .setDefaultTTL(3600)
        .build();

    assertThat(builder.getTranscoder())
        .isInstanceOf(TranscoderStub.class);
  }
}

class TranscoderStub implements Transcoder<String> {
  @Override
  public boolean asyncDecode(CachedData d) {
    return false;
  }

  @Override
  public String decode(CachedData d) {
    return new String(d.getData());
  }

  @Override
  public int getMaxSize() {
    return Integer.MAX_VALUE;
  }

  @Override
  public CachedData encode(String o) {
    return new CachedData(0, o.getBytes(), getMaxSize());
  }
}
