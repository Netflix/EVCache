package com.netflix.evcache;

import brave.Tracing;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
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

  /** The live MutableSpan handed to the reporter, kept by reference so later writes are visible. */
  List<MutableSpan> handedOffSpans;

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
            .thenReturn(Arrays.asList(new EVCacheKey("dummyAppName", "dummyKey", "dummyCanonicalKey", null, null, null, null)));
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
    handedOffSpans = new ArrayList<>();
    Tracing tracing =
            Tracing.newBuilder()
                    .addSpanHandler(
                            new SpanHandler() {
                              @Override
                              public boolean end(TraceContext context, MutableSpan span, Cause cause) {
                                handedOffSpans.add(span);
                                return true;
                              }
                            })
                    .spanReporter(reportedSpans::add)
                    .build();

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
    Assert.assertTrue(tags.containsKey(EVCacheTracingTags.CANONICAL_KEYS), "CANONICAL_KEYS tag is missing");
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

  /**
   * A later callback must not touch a span that has already been handed off. See
   * EVCacheTracingEventListener#onFinishHelper for the paths that fire both.
   *
   * <p>Asserts on the handed-off MutableSpan, not the reported zipkin2.Span: the conversion happens
   * at report time, so a post-finish write is invisible there.
   */
  @Test
  public void testOnErrorAfterOnCompleteDoesNotMutateFinishedSpan() {
    tracingListener.onStart(mockEVCacheEvent);
    tracingListener.onComplete(mockEVCacheEvent);

    Assert.assertEquals(handedOffSpans.size(), 1, "Expected exactly one span to be handed off");
    MutableSpan handedOff = handedOffSpans.get(0);
    int tagCountAtHandoff = handedOff.tagCount();

    tracingListener.onError(mockEVCacheEvent, new RuntimeException("Unexpected Error"));

    Assert.assertEquals(
            handedOff.tagCount(), tagCountAtHandoff, "A tag was added after the span was handed off");
    Assert.assertNull(
            handedOff.tag(EVCacheTracingTags.ERROR),
            "ERROR tag was written to a span that was already finished");
    Assert.assertEquals(
            handedOffSpans.size(), 1, "The span was handed off more than once");
  }

  /**
   * The claim is order independent: whichever callback arrives first owns the span.
   *
   * <p>This direction adds no new tag key, so a tag count alone cannot detect a second pass. The
   * status is changed between the callbacks to make one show up as an overwritten value.
   */
  @Test
  public void testOnCompleteAfterOnErrorDoesNotMutateFinishedSpan() {
    tracingListener.onStart(mockEVCacheEvent);
    tracingListener.onError(mockEVCacheEvent, new RuntimeException("Unexpected Error"));

    Assert.assertEquals(handedOffSpans.size(), 1, "Expected exactly one span to be handed off");
    MutableSpan handedOff = handedOffSpans.get(0);
    int tagCountAtHandoff = handedOff.tagCount();
    String statusAtHandoff = handedOff.tag(EVCacheTracingTags.STATUS);

    when(mockEVCacheEvent.getStatus()).thenReturn("statusWrittenAfterHandoff");
    tracingListener.onComplete(mockEVCacheEvent);

    Assert.assertEquals(
            handedOff.tagCount(), tagCountAtHandoff, "A tag was added after the span was handed off");
    Assert.assertEquals(
            handedOff.tag(EVCacheTracingTags.STATUS),
            statusAtHandoff,
            "STATUS tag was overwritten on a span that was already finished");
    Assert.assertEquals(handedOffSpans.size(), 1, "The span was handed off more than once");

    // The first callback still recorded everything it should have.
    verifyCommonTags(reportedSpans);
    verifyErrorTags(reportedSpans);
  }
}
