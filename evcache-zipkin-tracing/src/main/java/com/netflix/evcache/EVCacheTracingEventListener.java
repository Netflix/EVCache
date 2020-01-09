package com.netflix.evcache;

import brave.Span;
import brave.Tracer;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import net.spy.memcached.CachedData;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Adds tracing tags for EvCache calls. */
public class EVCacheTracingEventListener implements EVCacheEventListener {

  public static String EVCACHE_SPAN_NAME = "evcache";

  private static Logger logger = LoggerFactory.getLogger(EVCacheTracingEventListener.class);

  private static String CLIENT_SPAN_ATTRIBUTE_KEY = "clientSpanAttributeKey";

  private final Tracer tracer;

  public EVCacheTracingEventListener(EVCacheClientPoolManager poolManager, Tracer tracer) {
    poolManager.addEVCacheEventListener(this);
    this.tracer = tracer;
  }

  @Override
  public void onStart(EVCacheEvent e) {
    try {
      Span clientSpan =
          this.tracer.nextSpan().kind(Span.Kind.CLIENT).name(EVCACHE_SPAN_NAME).start(e.getStartTime());

      // Return if tracing has been disabled
      if(clientSpan.isNoop()){
        return;
      }

      String appName = e.getAppName();
      this.safeTag(clientSpan, EVCacheTracingTags.APP_NAME, appName);

      String cacheNamePrefix = e.getCacheName();
      this.safeTag(clientSpan, EVCacheTracingTags.CACHE_NAME_PREFIX, cacheNamePrefix);

      String call = e.getCall().name();
      this.safeTag(clientSpan, EVCacheTracingTags.CALL, call);

      /**
       * Note - e.getClients() returns a list of clients associated with the EVCacheEvent.
       *
       * <p>Read operation will have only 1 EVCacheClient as reading from just 1 instance of cache
       * is sufficient. Write operations will have appropriate number of clients as each client will
       * attempt to write to its cache instance.
       */
      String serverGroup;
      List<String> serverGroups = new ArrayList<>();
      for (EVCacheClient client : e.getClients()) {
        serverGroup = client.getServerGroupName();
        if (StringUtils.isNotBlank(serverGroup)) {
          serverGroups.add("\"" + serverGroup + "\"");
        }
      }
      clientSpan.tag(EVCacheTracingTags.SERVER_GROUPS, serverGroups.stream().collect(Collectors.joining(",", "[", "]")));

      /**
       * Note - EVCache client creates a hash key if the given canonical key size exceeds 255
       * characters.
       *
       * <p>There have been cases where canonical key size exceeded few megabytes. As caching client
       * creates a hash of such canonical keys and optimizes the storage in the cache servers, it is
       * safe to annotate hash key instead of canonical key in such cases.
       */
      String hashKey;
      List<String> hashKeys = new ArrayList<>();
      List<String> canonicalKeys = new ArrayList<>();
      for (EVCacheKey keyObj : e.getEVCacheKeys()) {
        hashKey = keyObj.getHashKey();
        if (StringUtils.isNotBlank(hashKey)) {
          hashKeys.add("\"" + hashKey + "\"");
        } else {
          canonicalKeys.add("\"" + keyObj.getCanonicalKey() + "\"");
        }
      }

      if(hashKeys.size() > 0) {
        this.safeTag(clientSpan, EVCacheTracingTags.HASH_KEYS,
                hashKeys.stream().collect(Collectors.joining(",", "[", "]")));
      }

      if(canonicalKeys.size() > 0) {
        this.safeTag(clientSpan, EVCacheTracingTags.CANONICAL_KEYS,
                canonicalKeys.stream().collect(Collectors.joining(",", "[", "]")));
      }

      /**
       * Note - tracer.spanInScope(...) method stores Spans in the thread local object.
       *
       * <p>As EVCache write operations are asynchronous and quorum based, we are avoiding attaching
       * clientSpan with tracer.spanInScope(...) method. Instead, we are storing the clientSpan as
       * an object in the EVCacheEvent's attributes.
       */
      e.setAttribute(CLIENT_SPAN_ATTRIBUTE_KEY, clientSpan);
    } catch (Exception exception) {
      logger.error("onStart exception", exception);
    }
  }

  @Override
  public void onComplete(EVCacheEvent e) {
    try {
      this.onFinishHelper(e, null);
    } catch (Exception exception) {
      logger.error("onComplete exception", exception);
    }
  }

  @Override
  public void onError(EVCacheEvent e, Throwable t) {
    try {
      this.onFinishHelper(e, t);
    } catch (Exception exception) {
      logger.error("onError exception", exception);
    }
  }

  /**
   * On throttle is not a trace event, but it is used to decide whether to throttle. We don't want
   * to interfere so always return false.
   */
  @Override
  public boolean onThrottle(EVCacheEvent e) throws EVCacheException {
    return false;
  }

  private void onFinishHelper(EVCacheEvent e, Throwable t) {
    Object clientSpanObj = e.getAttribute(CLIENT_SPAN_ATTRIBUTE_KEY);

    // Return if the previously saved Client Span is null
    if (clientSpanObj == null) {
      return;
    }

    Span clientSpan = (Span) clientSpanObj;

    try {
      if (t != null) {
        this.safeTag(clientSpan, EVCacheTracingTags.ERROR, t.toString());
      }

      String status = e.getStatus();
      this.safeTag(clientSpan, EVCacheTracingTags.STATUS, status);

      long latency = e.getDurationInMillis();
      clientSpan.tag(EVCacheTracingTags.LATENCY, String.valueOf(latency));

      int ttl = e.getTTL();
      clientSpan.tag(EVCacheTracingTags.DATA_TTL, String.valueOf(ttl));

      CachedData cachedData = e.getCachedData();
      if (cachedData != null) {
        int cachedDataSize = cachedData.getData().length;
        clientSpan.tag(EVCacheTracingTags.DATA_SIZE, String.valueOf(cachedDataSize));
      }
    } finally {
      clientSpan.finish();
    }
  }

  private void safeTag(Span span, String key, String value) {
    if (StringUtils.isNotBlank(value)) {
      span.tag(key, value);
    }
  }
}
