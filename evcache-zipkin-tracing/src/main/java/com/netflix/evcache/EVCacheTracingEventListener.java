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
import java.util.concurrent.atomic.AtomicReference;
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
              this.tracer.nextSpan().kind(Span.Kind.CLIENT).name(EVCACHE_SPAN_NAME).start();

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
       *
       * <p>The span is wrapped in a {@link PendingSpan} so only the first of onComplete/onError
       * finishes it. See {@link #onFinishHelper}.
       */
      e.setAttribute(CLIENT_SPAN_ATTRIBUTE_KEY, new PendingSpan(clientSpan));
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

  /**
   * Tags and finishes the span for this event, at most once.
   *
   * <p>EVCacheImpl fires both onComplete and onError for one event on several paths, so without the
   * claim below the second callback mutates a span the reporter may already be encoding:
   *
   * <ul>
   *   <li>async get -- handleMissData (endEvent) then handleException (eventError), in the same
   *       CompletableFuture.handle(...) branch
   *   <li>async bulk get -- handleFullCacheMiss then handleException, likewise
   *   <li>append -- endEvent, then touchData(...) inside the same try whose catch calls eventError
   *   <li>getAndTouch -- eventError twice in a row
   * </ul>
   *
   * <p>The first callback wins, so an error reported by a later one is dropped. Recovering it means
   * not firing both callbacks in EVCacheImpl.
   */
  private void onFinishHelper(EVCacheEvent e, Throwable t) {
    Object clientSpanObj = e.getAttribute(CLIENT_SPAN_ATTRIBUTE_KEY);

    // Also covers null. The attribute map is string-keyed and shared, so check the type.
    if (!(clientSpanObj instanceof PendingSpan)) {
      return;
    }

    // Whoever claims it finishes it; any later callback for this event gets null and is a no-op.
    Span clientSpan = ((PendingSpan) clientSpanObj).claim();
    if (clientSpan == null) {
      return;
    }

    try {
      if (t != null) {
        this.safeTag(clientSpan, EVCacheTracingTags.ERROR, t.toString());
      }

      String status = e.getStatus();
      this.safeTag(clientSpan, EVCacheTracingTags.STATUS, status);

      long latency = this.getDurationInMicroseconds(e.getDurationInMillis());
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

  /**
   * A span that can be claimed exactly once.
   *
   * <p>EVCacheEvent's attribute map is an unsynchronized HashMap, so the claim cannot be a
   * remove-and-check on the map itself.
   */
  private static final class PendingSpan extends AtomicReference<Span> {

    PendingSpan(Span span) {
      super(span);
    }

    /** Returns the span to the first caller only; null for every caller after that. */
    Span claim() {
      return this.getAndSet(null);
    }
  }

  private long getDurationInMicroseconds(long durationInMillis) {

    // EVCacheEvent returns durationInMillis as -1 if endTime is not available.
    if(durationInMillis == -1){
      return durationInMillis;
    } else {
      // Since the underlying EVCacheEvent returns duration in milliseconds we already
      // lost the required precision for conversion to microseconds. Multiplication
      // by 1000 should suffice here.
      return durationInMillis * 1000;
    }
  }
}
