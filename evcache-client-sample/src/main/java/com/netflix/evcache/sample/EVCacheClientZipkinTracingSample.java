package com.netflix.evcache.sample;

import brave.Tracing;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheTracingEventListener;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class EVCacheClientZipkinTracingSample {
  private final EVCache evCache;
  private final List<Span> reportedSpans;
  private static boolean verboseMode = false;

  /**
   * Default constructor.
   *
   * <p>This tells the EVCache library to use the "simple node list provider" for EVCACHE_APP1 (by
   * setting the relevant system property), and then it copies the EVC_SAMPLE_DEPLOYMENT environment
   * variable to the EVCACHE_APP1-NODES system property.
   *
   * <p>If the environment variable isn't set, default memcached server is at localhost:11211.
   *
   * <p>Finally, this initializes "evCache" using EVCache.Builder, specifying the application name
   * "EVCACHE_APP1."
   */
  public EVCacheClientZipkinTracingSample() {
    String deploymentDescriptor = System.getenv("EVC_SAMPLE_DEPLOYMENT");
    if (deploymentDescriptor == null) {
      // No deployment descriptor in the environment, use a defaul.
      deploymentDescriptor = "SERVERGROUP1=localhost:11211";
    }
    System.setProperty("EVCACHE_APP1.use.simple.node.list.provider", "true");
    System.setProperty("EVCACHE_APP1-NODES", deploymentDescriptor);

    EVCacheClientPoolManager poolManager = EVCacheClientPoolManager.getInstance();
    poolManager.initEVCache("EVCACHE_APP1");

    reportedSpans = new ArrayList<>();
    Tracing tracing = Tracing.newBuilder().spanReporter(reportedSpans::add).build();
    EVCacheTracingEventListener tracingEventListener =
        new EVCacheTracingEventListener(poolManager, tracing.tracer());

    evCache = new EVCache.Builder().setAppName("EVCACHE_APP1").build();
  }

  /**
   * Set a key in the cache.
   *
   * <p>See the memcached documentation for what "timeToLive" means. Zero means "never expires."
   * Small integers (under some threshold) mean "expires this many seconds from now." Large integers
   * mean "expires at this Unix timestamp" (seconds since 1/1/1970). Warranty expires 17-Jan 2038.
   */
  public void setKey(String key, String value, int timeToLive) throws Exception {
    try {

      Future<Boolean>[] _future = evCache.set(key, value, timeToLive);

      // Wait for all the Futures to complete.
      // In "verbose" mode, show the status for each.
      for (Future<Boolean> f : _future) {
        boolean didSucceed = f.get();
        if (verboseMode) {
          System.out.println("per-shard set success code for key " + key + " is " + didSucceed);
        }
      }
      if (!verboseMode) {
        // Not verbose. Just give one line of output per "set," without a success code
        System.out.println("finished setting key " + key);
      }
    } catch (EVCacheException e) {
      e.printStackTrace();
    }
  }

  /**
   * Get the data for a key from the cache. Returns null if the key could not be retrieved, whether
   * due to a cache miss or errors.
   */
  public String getKey(String key) {
    try {
      String _response = evCache.<String>get(key);
      return _response;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public void printZipkinSpans() {
    System.out.println("--> " + reportedSpans.toString());
  }

  /** Main Program which does some simple sets and gets. */
  public static void main(String[] args) {
    // set verboseMode based on the environment variable
    verboseMode = ("true".equals(System.getenv("EVCACHE_SAMPLE_VERBOSE")));

    if (verboseMode) {
      System.out.println("To run this sample app without using Gradle:");
      System.out.println(
          "java -cp "
              + System.getProperty("java.class.path")
              + " com.netflix.evcache.sample.EVCacheClientZipkinTracingSample");
    }

    try {
      EVCacheClientZipkinTracingSample evCacheClientZipkinTracingSample =
          new EVCacheClientZipkinTracingSample();

      // Set ten keys to different values
      for (int i = 0; i < 10; i++) {
        String key = "key_" + i;
        String value = "data_" + i;
        // Set the TTL to 24 hours
        int ttl = 86400;
        evCacheClientZipkinTracingSample.setKey(key, value, ttl);
      }

      // Do a "get" for each of those same keys
      for (int i = 0; i < 10; i++) {
        String key = "key_" + i;
        String value = evCacheClientZipkinTracingSample.getKey(key);
        System.out.println("Get of " + key + " returned " + value);
      }

      // Print collected Zipkin Spans
      evCacheClientZipkinTracingSample.printZipkinSpans();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // We have to call System.exit() now, because some background
    // threads were started without the "daemon" flag. This is
    // probably a mistake somewhere, but hey, this is only a sample app.
    System.exit(0);
  }
}
