package com.netflix.evcache;

import java.time.Duration;

public class EVCacheClientPoolConfigurationProperties {
  /**
   * Prefix to be applied to keys.
   */
  private String keyPrefix;

  /**
   * Time-to-live in seconds.
   */
  private Duration timeToLive;

  /**
   * Whether or not retry is to be enabled.
   */
  private Boolean retryEnabled = true;

  /**
   * Whether or not exception throwing is to be enabled.
   */
  private Boolean exceptionThrowingEnabled = false;

  /**
   * FQPN to `Transcoder`.
   */
  private String transcoder;

  public EVCacheClientPoolConfigurationProperties() {
    this.keyPrefix = "";
    this.timeToLive = Duration.ofSeconds(900);
    this.retryEnabled = true;
    this.exceptionThrowingEnabled = false;
    this.transcoder = null;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }

  public void setKeyPrefix(String keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public Duration getTimeToLive() {
    return timeToLive;
  }

  public void setTimeToLive(Duration timeToLive) {
    this.timeToLive = timeToLive;
  }

  public Boolean getRetryEnabled() {
    return retryEnabled;
  }

  public void setRetryEnabled(Boolean retryEnabled) {
    this.retryEnabled = retryEnabled;
  }

  public Boolean getExceptionThrowingEnabled() {
    return exceptionThrowingEnabled;
  }

  public void setExceptionThrowingEnabled(Boolean exceptionThrowingEnabled) {
    this.exceptionThrowingEnabled = exceptionThrowingEnabled;
  }

  public String getTranscoder() {
    return transcoder;
  }

  public <T> void setTranscoder(String transcoder) {
    this.transcoder = transcoder;
  }
}
