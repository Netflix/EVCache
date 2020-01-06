package com.netflix.evcache;

import brave.Tracing;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import net.spy.memcached.CachedData;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class EVCacheTracingEventListenerUnitTests {

  List<zipkin2.Span> reportedSpans;
  EVCacheTracingEventListener tracingListener;
  EVCacheClient mockEVCacheClient;
  EVCacheEvent mockEVCacheEvent;

  @BeforeMethod
  public void resetMocks() {
    mockEVCacheClient = mock(EVCacheClient.class);
    when(mockEVCacheClient.getServerGroupName()).thenReturn("dummyServerGroupName");

    mockEVCacheEvent = mock(EVCacheEvent.class);

    when(mockEVCacheEvent.getClients()).thenReturn(Arrays.asList(mockEVCacheClient));
    when(mockEVCacheEvent.getCall()).thenReturn(EVCache.Call.GET);

    when(mockEVCacheEvent.getAppName()).thenReturn("dummyAppName");
    when(mockEVCacheEvent.getCacheName()).thenReturn("dummyCacheName");
    when(mockEVCacheEvent.getEVCacheKeys())
        .thenReturn(Arrays.asList(new EVCacheKey("dummyKey", "dummyCanonicalKey", null)));
    when(mockEVCacheEvent.getStatus()).thenReturn("success");
    when(mockEVCacheEvent.getDurationInMillis()).thenReturn(1L);
    when(mockEVCacheEvent.getTTL()).thenReturn(0);
    when(mockEVCacheEvent.getCachedData())
        .thenReturn(new CachedData(1, "dummyData".getBytes(), 255));

    Map<String, Object> eventAttributes = new HashMap<>();
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                String key = (String) arguments[0];
                Object value = arguments[1];
                eventAttributes.put(key, value);
                return null;
              }
            })
        .when(mockEVCacheEvent)
        .setAttribute(any(), any());

    doAnswer(
            new Answer<Object>() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                String key = (String) arguments[0];
                return eventAttributes.get(key);
              }
            })
        .when(mockEVCacheEvent)
        .getAttribute(any());

    reportedSpans = new ArrayList<>();
    Tracing tracing = Tracing.newBuilder().spanReporter(reportedSpans::add).build();

    tracingListener =
        new EVCacheTracingEventListener(mock(EVCacheClientPoolManager.class), tracing.tracer());
  }

  public void verifyCommonTags(List<zipkin2.Span> spans) {
    Assert.assertEquals(spans.size(), 1, "Number of expected spans are not matching");
    zipkin2.Span span = spans.get(0);

    Assert.assertEquals(span.kind(), Span.Kind.CLIENT, "Span Kind are not equal");
    Assert.assertEquals(
        span.name(), EVCacheTracingEventListener.EVCACHE_SPAN_NAME, "Cache name are not equal");

    Map<String, String> tags = span.tags();
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.APP_NAME), "APP_NAME tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.CACHE_NAME_PREFIX), "CACHE_NAME_PREFIX tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.CALL), "CALL tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.SERVER_GROUPS), "SERVER_GROUPS tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.CANONICAL_KEY), "CANONICAL_KEY tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.STATUS), "STATUS tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.LATENCY), "LATENCY tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.DATA_TTL), "DATA_TTL tag is missing");
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.DATA_SIZE), "DATA_SIZE tag is missing");
  }

  public void verifyErrorTags(List<zipkin2.Span> spans) {
    zipkin2.Span span = spans.get(0);
    Map<String, String> tags = span.tags();
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.ERROR), "ERROR tag is missing");
  }

  @Test
  public void testEVCacheListenerOnComplete() {
    tracingListener.onStart(mockEVCacheEvent);
    tracingListener.onComplete(mockEVCacheEvent);

    verifyCommonTags(reportedSpans);
  }

  @Test
  public void testEVCacheListenerOnError() {
    tracingListener.onStart(mockEVCacheEvent);
    tracingListener.onError(mockEVCacheEvent, new RuntimeException("Unexpected Error"));

    verifyCommonTags(reportedSpans);
    verifyErrorTags(reportedSpans);
  }
}
