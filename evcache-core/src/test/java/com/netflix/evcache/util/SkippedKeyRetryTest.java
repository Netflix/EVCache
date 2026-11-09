package com.netflix.evcache.util;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.testng.annotations.Test;

import com.netflix.evcache.pool.EVCacheClient;

public class SkippedKeyRetryTest {

    private static final List<String> KEYS = Arrays.asList("item", "version");

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** A fetch that answers from a per-client map and records which keys each client was asked for. */
    private static BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetchFrom(
            Map<EVCacheClient, Map<String, String>> data, Map<EVCacheClient, List<String>> asked) {
        return (client, keys) -> {
            asked.put(client, new ArrayList<>(keys));
            Map<String, String> source = data.getOrDefault(client, Collections.<String, String>emptyMap());
            Map<String, String> result = new HashMap<>();
            for (String k : keys) {
                if (source.containsKey(k)) {
                    result.put(k, source.get(k));
                }
            }
            return CompletableFuture.completedFuture(result);
        };
    }

    @Test
    public void earlyRetryKeysAreTheSkippedKeysInRequestOrder() {
        Set<String> skipped = new HashSet<>(Arrays.asList("d", "b"));
        // "a", "c" and "e" were sent (they may have hit or genuinely missed) so they are not retried
        assertEquals(SkippedKeyRetry.keysToRetryEarly(Arrays.asList("a", "b", "c", "d", "e"), skipped),
                Arrays.asList("b", "d"));
    }

    @Test
    public void duplicateKeysInTheRequestAreHandledOnce() {
        // a is skipped, b is not: the duplicate must neither be fetched twice nor hide that b was sent
        assertEquals(SkippedKeyRetry.keysToRetryEarly(Arrays.asList("a", "a", "b"), new HashSet<>(Arrays.asList("a"))),
                Arrays.asList("a"));
        // every unique key skipped, even though the list is longer: left to the full retry
        assertTrue(SkippedKeyRetry.keysToRetryEarly(Arrays.asList("a", "a", "b"), new HashSet<>(Arrays.asList("a", "b"))).isEmpty());
    }

    @Test
    public void earlyRetryIsLeftToTheFullRetryWhenEveryKeyWasSkipped() {
        Set<String> skipped = new HashSet<>(Arrays.asList("a", "b"));
        assertTrue(SkippedKeyRetry.keysToRetryEarly(Arrays.asList("a", "b"), skipped).isEmpty());
    }

    @Test
    public void noEarlyRetryWhenNothingWasSkippedOrTrackingIsOff() {
        assertTrue(SkippedKeyRetry.keysToRetryEarly(KEYS, Collections.<String>emptySet()).isEmpty());
        assertTrue(SkippedKeyRetry.keysToRetryEarly(KEYS, null).isEmpty());
    }

    @Test
    public void mergeKeepsFoundValuesAndAddsRecoveredOnes() {
        Map<String, String> recovered = map("item", "i-other", "version", "v-other");
        recovered.put("nothing", null);
        Map<String, String> merged = SkippedKeyRetry.merge(map("item", "i"), recovered);
        assertEquals(merged.get("item"), "i");
        assertEquals(merged.get("version"), "v-other");
        assertTrue(!merged.containsKey("nothing"));
    }

    @Test
    public void mergeFillsNullValuedEntriesAndToleratesNulls() {
        Map<String, String> found = new HashMap<>();
        found.put("version", null);
        assertEquals(SkippedKeyRetry.merge(found, map("version", "v")).get("version"), "v");
        assertEquals(SkippedKeyRetry.merge(null, null).size(), 0);
        assertEquals(SkippedKeyRetry.merge(map("a", "1"), null), map("a", "1"));
    }

    @Test
    public void stopsWhenShouldStopIsTrueAndKeepsWhatItHas() throws Exception {
        EVCacheClient other = mock(EVCacheClient.class);
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger attempts = new AtomicInteger();

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Collections.singletonList(other),
                fetchFrom(new HashMap<>(), asked), attempts::incrementAndGet, t -> { }, () -> true).get();

        assertEquals(result, map("item", "i"));
        assertTrue(asked.isEmpty());
        assertEquals(attempts.get(), 0);
    }

    @Test(expectedExceptions = OutOfMemoryError.class)
    public void errorsAreNotSwallowed() {
        EVCacheClient client = mock(EVCacheClient.class);
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (c, keys) -> {
            throw new OutOfMemoryError("test");
        };
        SkippedKeyRetry.retryMissing(map("item", "i"), KEYS, Collections.singletonList(client), fetch, () -> { }, t -> { }, () -> false);
    }

    @Test
    public void fillsOnlyTheMissingKeyFromTheFirstFallbackClient() throws Exception {
        EVCacheClient other = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(other, map("version", "v-other"));
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger attempts = new AtomicInteger();

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i-local"), KEYS, Collections.singletonList(other),
                fetchFrom(data, asked), attempts::incrementAndGet, t -> { }, () -> false).get();

        assertEquals(result.get("item"), "i-local");
        assertEquals(result.get("version"), "v-other");
        assertEquals(asked.get(other), Collections.singletonList("version"));
        assertEquals(attempts.get(), 1);
    }

    @Test
    public void aCandidateKeyListedTwiceIsAskedForOnce() throws Exception {
        EVCacheClient other = mock(EVCacheClient.class);
        Map<EVCacheClient, List<String>> asked = new HashMap<>();

        SkippedKeyRetry.retryMissing(new HashMap<String, String>(), Arrays.asList("a", "b", "a"), Collections.singletonList(other),
                fetchFrom(new HashMap<>(), asked), () -> { }, t -> { }, () -> false).get();

        assertEquals(asked.get(other), Arrays.asList("a", "b"));
    }

    @Test
    public void aKeyFoundWithANullValueIsFilledFromTheFallback() throws Exception {
        EVCacheClient other = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(other, map("version", "v-other"));
        Map<String, String> found = map("item", "i");
        found.put("version", null);

        Map<String, String> result = SkippedKeyRetry.retryMissing(found, KEYS, Collections.singletonList(other),
                fetchFrom(data, new HashMap<>()), () -> { }, t -> { }, () -> false).get();

        assertEquals(result.get("version"), "v-other");
        assertEquals(result.get("item"), "i");
    }

    @Test
    public void doesNotOverwriteValuesAlreadyFound() throws Exception {
        EVCacheClient other = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(other, map("item", "i-other", "version", "v-other"));

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i-local"), KEYS, Collections.singletonList(other),
                fetchFrom(data, new HashMap<>()), () -> { }, t -> { }, () -> false).get();

        assertEquals(result.get("item"), "i-local");
        assertEquals(result.get("version"), "v-other");
    }

    @Test
    public void stopsOnceEveryKeyHasAValue() throws Exception {
        EVCacheClient first = mock(EVCacheClient.class);
        EVCacheClient second = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(first, map("version", "v1"));
        data.put(second, map("version", "v2"));
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger attempts = new AtomicInteger();

        SkippedKeyRetry.retryMissing(map("item", "i"), KEYS, Arrays.asList(first, second),
                fetchFrom(data, asked), attempts::incrementAndGet, t -> { }, () -> false).get();

        assertTrue(asked.containsKey(first));
        assertTrue(!asked.containsKey(second));
        assertEquals(attempts.get(), 1);
    }

    @Test
    public void movesToTheNextClientAndAsksOnlyForWhatIsStillMissing() throws Exception {
        EVCacheClient first = mock(EVCacheClient.class);
        EVCacheClient second = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(first, map("a", "a1"));
        data.put(second, map("b", "b2", "c", "c2"));
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger attempts = new AtomicInteger();

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                new HashMap<String, String>(), Arrays.asList("a", "b", "c"), Arrays.asList(first, second),
                fetchFrom(data, asked), attempts::incrementAndGet, t -> { }, () -> false).get();

        assertEquals(asked.get(first), Arrays.asList("a", "b", "c"));
        assertEquals(asked.get(second), Arrays.asList("b", "c"));
        assertEquals(result.size(), 3);
        assertEquals(attempts.get(), 2);
    }

    @Test
    public void aFailingClientIsSkippedAndThePartialResultIsKept() throws Exception {
        EVCacheClient broken = mock(EVCacheClient.class);
        EVCacheClient working = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(working, map("version", "v"));
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> good =
                fetchFrom(data, new HashMap<>());
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (client, keys) -> {
            if (client == broken) {
                CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("boom"));
                return failed;
            }
            return good.apply(client, keys);
        };

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Arrays.asList(broken, working), fetch, () -> { }, t -> { }, () -> false).get();

        assertEquals(result.get("item"), "i");
        assertEquals(result.get("version"), "v");
    }

    @Test
    public void aFetchThatThrowsSynchronouslyIsTreatedAsAFailure() throws Exception {
        EVCacheClient client = mock(EVCacheClient.class);
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (c, keys) -> {
            throw new IllegalStateException("throttled");
        };

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Collections.singletonList(client), fetch, () -> { }, t -> { }, () -> false).get();

        assertEquals(result.get("item"), "i");
        assertNull(result.get("version"));
    }

    @Test
    public void noFallbackClientsReturnsTheInputUnchanged() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        Map<String, String> input = map("item", "i");

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                input, KEYS, Collections.<EVCacheClient>emptyList(),
                fetchFrom(new HashMap<>(), new HashMap<>()), attempts::incrementAndGet, t -> { }, () -> false).get();

        assertEquals(result, input);
        assertEquals(attempts.get(), 0);
    }

    @Test
    public void nothingMissingMeansNoFetch() throws Exception {
        EVCacheClient client = mock(EVCacheClient.class);
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger attempts = new AtomicInteger();

        SkippedKeyRetry.retryMissing(map("item", "i", "version", "v"), KEYS, Collections.singletonList(client),
                fetchFrom(new HashMap<>(), asked), attempts::incrementAndGet, t -> { }, () -> false).get();

        assertTrue(asked.isEmpty());
        assertEquals(attempts.get(), 0);
    }

    @Test
    public void skippedKeySetOnlyExistsWhenBothZoneFallbackAndTheFlagAreOn() {
        assertTrue(SkippedKeyRetry.<String>newSkippedKeySet(true, true) != null);
        assertNull(SkippedKeyRetry.<String>newSkippedKeySet(true, false));
        assertNull(SkippedKeyRetry.<String>newSkippedKeySet(false, true));
        assertNull(SkippedKeyRetry.<String>newSkippedKeySet(false, false));
    }

    @Test
    public void skippedRawStringsMapBackToPlainAndHashedKeys() {
        Map<String, String> plain = map("a", "KA");
        Map<String, String> hashed = map("h1", "KB");
        Set<String> out = new HashSet<>();

        SkippedKeyRetry.addSkippedKeys(new HashSet<>(Arrays.asList("a", "h1", "zzz")), plain, hashed, out);

        assertEquals(out, new HashSet<>(Arrays.asList("KA", "KB")));
    }

    @Test
    public void checksShouldStopBeforeEveryAttempt() throws Exception {
        EVCacheClient first = mock(EVCacheClient.class);
        EVCacheClient second = mock(EVCacheClient.class);
        EVCacheClient third = mock(EVCacheClient.class);
        Map<EVCacheClient, List<String>> asked = new HashMap<>();
        AtomicInteger checks = new AtomicInteger();
        AtomicInteger attempts = new AtomicInteger();

        // allows the first attempt, then asks to stop; nothing is found so the keys stay missing
        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Arrays.asList(first, second, third),
                fetchFrom(new HashMap<>(), asked), attempts::incrementAndGet, t -> { },
                () -> checks.incrementAndGet() > 1).get();

        assertEquals(asked.size(), 1);
        assertTrue(asked.containsKey(first));
        assertEquals(attempts.get(), 1);
        assertEquals(result, map("item", "i"));
    }

    @Test
    public void countsEveryAttemptAndEveryFailure() throws Exception {
        EVCacheClient broken = mock(EVCacheClient.class);
        EVCacheClient thrower = mock(EVCacheClient.class);
        EVCacheClient working = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(working, map("version", "v"));
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> good =
                fetchFrom(data, new HashMap<>());
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (client, keys) -> {
            if (client == thrower) throw new IllegalStateException("sync");
            if (client == broken) {
                CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("async"));
                return failed;
            }
            return good.apply(client, keys);
        };
        AtomicInteger attempts = new AtomicInteger();
        List<Throwable> failures = new ArrayList<>();

        Map<String, String> result = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Arrays.asList(broken, thrower, working), fetch,
                attempts::incrementAndGet, failures::add, () -> false).get();

        assertEquals(attempts.get(), 3);
        assertEquals(failures.size(), 2);
        assertEquals(result.get("version"), "v");
    }

    @Test
    public void recoveryFromAFailedFetchYieldsTheFallbackButNeverSwallowsAnError() {
        Map<String, String> fallback = new HashMap<>();
        assertTrue(SkippedKeyRetry.recoverFromFailure(new RuntimeException("x"), fallback) == fallback);
        assertTrue(SkippedKeyRetry.recoverFromFailure(
                new java.util.concurrent.CompletionException(new RuntimeException("x")), fallback) == fallback);
        for (Throwable error : new Throwable[] { new OutOfMemoryError("x"),
                new java.util.concurrent.CompletionException(new StackOverflowError()) }) {
            try {
                SkippedKeyRetry.recoverFromFailure(error, fallback);
                org.testng.Assert.fail("an Error must propagate");
            } catch (Error expected) {
                if (expected instanceof AssertionError && "an Error must propagate".equals(expected.getMessage())) throw expected;
            }
        }
    }

    // This is about retryMissing on its own. EVCacheImpl recovers from failed fetches with recoverFromFailure above,
    // which also lets an Error through, so the two agree.
    @Test
    public void anErrorFromALaterAttemptFailsTheFutureInsteadOfBeingSwallowed() {
        EVCacheClient first = mock(EVCacheClient.class);
        EVCacheClient second = mock(EVCacheClient.class);
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (client, keys) -> {
            if (client == second) throw new OutOfMemoryError("test");
            return CompletableFuture.completedFuture(new HashMap<String, String>());
        };

        CompletableFuture<Map<String, String>> future = SkippedKeyRetry.retryMissing(
                map("item", "i"), KEYS, Arrays.asList(first, second), fetch, () -> { }, t -> { }, () -> false);

        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void sentKeysAreTheOnesThatWereNotSkipped() {
        assertEquals(SkippedKeyRetry.sentKeys(Arrays.asList("a", "b", "c"), new HashSet<>(Arrays.asList("b"))),
                Arrays.asList("a", "c"));
        assertEquals(SkippedKeyRetry.sentKeys(KEYS, null), KEYS);
    }

    @Test
    public void sentKeysAreUniqueAndInRequestOrder() {
        assertEquals(SkippedKeyRetry.sentKeys(Arrays.asList("a", "b", "a", "c", "b"), new HashSet<>(Arrays.asList("c"))),
                Arrays.asList("a", "b"));
    }

    @Test
    public void countRecoveredOnlyCountsKeysThatWereMissing() {
        Map<String, String> found = map("a", "1");
        found.put("b", null);
        Map<String, String> recovered = map("a", "other", "b", "2", "c", "3");
        recovered.put("d", null);
        assertEquals(SkippedKeyRetry.countRecovered(found, recovered), 2); // b (was null) and c
        assertEquals(SkippedKeyRetry.countRecovered(null, null), 0);
    }

    @Test
    public void aThrowingOnFailureDoesNotDiscardWhatWasMerged() throws Exception {
        EVCacheClient broken = mock(EVCacheClient.class);
        EVCacheClient working = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(working, map("version", "v"));
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> good = fetchFrom(data, new HashMap<>());
        BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch = (c, keys) -> {
            if (c == broken) {
                CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("x"));
                return failed;
            }
            return good.apply(c, keys);
        };

        Map<String, String> result = SkippedKeyRetry.retryMissing(map("item", "i"), KEYS, Arrays.asList(broken, working),
                fetch, () -> { }, t -> { throw new IllegalStateException("onFailure blew up"); }, () -> false).get();

        assertEquals(result.get("version"), "v");
    }

    @Test
    public void recoveryUnwrapsNestedWrappersBeforeLookingForAnError() {
        Map<String, String> fallback = new HashMap<>();
        for (Throwable error : new Throwable[] {
                new java.util.concurrent.ExecutionException(new OutOfMemoryError("x")),
                new java.util.concurrent.CompletionException(new java.util.concurrent.CompletionException(new OutOfMemoryError("x"))),
                new java.util.concurrent.ExecutionException(new java.util.concurrent.CompletionException(new OutOfMemoryError("x"))) }) {
            boolean thrown = false;
            try {
                SkippedKeyRetry.recoverFromFailure(error, fallback);
            } catch (OutOfMemoryError expected) {
                thrown = true;
            }
            assertTrue(thrown, "an Error nested in wrappers must propagate");
        }
    }

    private static CompletableFuture<Map<String, String>> done(Map<String, String> m) {
        return CompletableFuture.completedFuture(m);
    }

    @Test
    public void aMainResultWithDataIsOnlyMergedAndNeverTriggersTheFullRetry() throws Exception {
        AtomicInteger fullRetries = new AtomicInteger();
        List<Long> recovered = new ArrayList<>();
        Map<String, String> result = SkippedKeyRetry.combine(map("item", "i"), map("version", "v"), true,
                () -> { fullRetries.incrementAndGet(); return done(map("x", "y")); }, recovered::add, t -> { }).get();

        assertEquals(result, map("item", "i", "version", "v"));
        assertEquals(fullRetries.get(), 0);
        assertEquals(recovered, Arrays.asList(1L));
    }

    @Test
    public void anEmptyMainResultRunsTheFullRetryAndMergesAllThreeSources() throws Exception {
        AtomicInteger fullRetries = new AtomicInteger();
        Map<String, String> result = SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true,
                () -> { fullRetries.incrementAndGet(); return done(map("item", "i", "version", "ignored")); }, n -> { }, t -> { }).get();

        assertEquals(fullRetries.get(), 1);
        assertEquals(result, map("item", "i", "version", "v"));
    }

    @Test
    public void noFullRetryWhenZoneFallbackIsOff() throws Exception {
        AtomicInteger fullRetries = new AtomicInteger();
        Map<String, String> result = SkippedKeyRetry.combine(null, map("version", "v"), false,
                () -> { fullRetries.incrementAndGet(); return done(map("x", "y")); }, n -> { }, t -> { }).get();

        assertEquals(fullRetries.get(), 0);
        assertEquals(result, map("version", "v"));
    }

    @Test
    public void aFullRetryThatFindsNothingKeepsTheShapeItAlwaysHad() throws Exception {
        // null in, null out: the caller turns that into an empty result, as before the skipped key fetch existed
        assertNull(SkippedKeyRetry.combine(new HashMap<String, String>(), new HashMap<String, String>(), true,
                () -> CompletableFuture.<Map<String, String>>completedFuture(null), n -> { }, t -> { }).get());
        // an empty map in, an empty map out: the caller turns that into "every key mapped to null"
        assertEquals(SkippedKeyRetry.combine(new HashMap<String, String>(), new HashMap<String, String>(), true,
                () -> done(new HashMap<String, String>()), n -> { }, t -> { }).get(), new HashMap<String, String>());
    }

    @Test
    public void aFullRetryThatBreaksKeepsRecoveredKeysOrElseFails() throws Exception {
        Supplier<CompletableFuture<Map<String, String>>> breaks = () -> {
            CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("x"));
            return failed;
        };
        assertEquals(SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true, breaks, n -> { }, t -> { }).get(),
                map("version", "v"));
        try {
            SkippedKeyRetry.combine(new HashMap<String, String>(), new HashMap<String, String>(), true, breaks, n -> { }, t -> { }).get();
            org.testng.Assert.fail("with nothing recovered the failure must propagate");
        } catch (java.util.concurrent.ExecutionException expected) {
            // as before the skipped key fetch existed
        }
        // a full retry that cannot even be started must not cost the recovered keys...
        assertEquals(SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true,
                () -> { throw new IllegalStateException("x"); }, n -> { }, t -> { }).get(), map("version", "v"));
        // ...and with nothing recovered it fails just like a full retry that breaks later on
        try {
            SkippedKeyRetry.combine(new HashMap<String, String>(), new HashMap<String, String>(), true,
                    () -> { throw new IllegalStateException("x"); }, n -> { }, t -> { }).get();
            org.testng.Assert.fail("with nothing recovered the failure must propagate");
        } catch (java.util.concurrent.ExecutionException expected) {
            assertTrue(expected.getCause() instanceof IllegalStateException);
        }
    }

    // ---- fetchFromOtherZone: the orchestration EVCacheImpl.startSkippedKeyFallback delegates to

    private static final class Probe {
        final AtomicInteger picks = new AtomicInteger(), noClient = new AtomicInteger(), attempts = new AtomicInteger(),
                failures = new AtomicInteger(), fetches = new AtomicInteger();
        final List<Long> retried = new ArrayList<>();
        final List<EVCacheClient> fetchedFrom = new ArrayList<>();
        final List<List<String>> fetchedKeys = new ArrayList<>();
    }

    private static CompletableFuture<Map<String, String>> startFetch(Probe p, List<String> keys, List<EVCacheClient> clients,
            BiFunction<EVCacheClient, List<String>, CompletableFuture<Map<String, String>>> fetch, boolean stop) {
        return SkippedKeyRetry.fetchFromOtherZone(keys,
                () -> { p.picks.incrementAndGet(); return clients.isEmpty() ? null : clients.get(0); },
                (c, k) -> { p.fetches.incrementAndGet(); p.fetchedFrom.add(c); p.fetchedKeys.add(new ArrayList<>(k)); return fetch.apply(c, k); },
                p.noClient::incrementAndGet, p.retried::add, p.attempts::incrementAndGet,
                t -> p.failures.incrementAndGet(), () -> stop);
    }

    @Test
    public void nothingToFetchMeansNoClientIsEvenPicked() {
        Probe p = new Probe();
        assertNull(startFetch(p, Collections.<String>emptyList(), Arrays.asList(mock(EVCacheClient.class)),
                (c, k) -> done(map()), false));
        assertEquals(p.picks.get(), 0);
    }

    @Test
    public void noOtherZoneMeansNothingStartsAndItIsCounted() {
        Probe p = new Probe();
        assertNull(startFetch(p, Arrays.asList("a"), Collections.<EVCacheClient>emptyList(), (c, k) -> done(map()), false));
        assertEquals(p.noClient.get(), 1);
        assertEquals(p.fetches.get(), 0);
        assertTrue(p.retried.isEmpty());
    }

    @Test
    public void exactlyOneCallIsMadeToTheOneClientThatWasPicked() throws Exception {
        Probe p = new Probe();
        EVCacheClient first = mock(EVCacheClient.class), second = mock(EVCacheClient.class);

        // the fetch finds nothing, which would make a loop over several clients keep going; there must still be one call
        Map<String, String> result = startFetch(p, Arrays.asList("a", "b"), Arrays.asList(first, second), (c, k) -> done(map()), false).get();

        assertEquals(p.picks.get(), 1);
        assertEquals(p.fetches.get(), 1);
        assertTrue(p.fetchedFrom.get(0) == first);
        assertEquals(p.fetchedKeys.get(0), Arrays.asList("a", "b"));
        assertEquals(p.retried, Arrays.asList(2L));
        assertEquals(p.attempts.get(), 1);
        assertTrue(result.isEmpty());
    }

    @Test
    public void theFetchIsStartedBeforeTheMethodReturns() {
        Probe p = new Probe();
        CompletableFuture<Map<String, String>> pending = new CompletableFuture<>();

        CompletableFuture<Map<String, String>> started = startFetch(p, Arrays.asList("a"),
                Arrays.asList(mock(EVCacheClient.class)), (c, k) -> pending, false);

        // this is what lets it run in parallel with the main call: the call is out before anything is awaited
        assertEquals(p.fetches.get(), 1);
        assertTrue(!started.isDone());
        pending.complete(map("a", "1"));
        assertEquals(started.join(), map("a", "1"));
    }

    @Test
    public void aFailedFetchYieldsAnEmptyResultAndIsCounted() throws Exception {
        Probe p = new Probe();
        Map<String, String> result = startFetch(p, Arrays.asList("a"), Arrays.asList(mock(EVCacheClient.class)), (c, k) -> {
            CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("x"));
            return failed;
        }, false).get();
        assertTrue(result.isEmpty());
        assertEquals(p.failures.get(), 1);
    }

    @Test
    public void anExceptionPickingTheClientStartsNothingAndDoesNotPropagate() {
        Probe p = new Probe();
        assertNull(SkippedKeyRetry.fetchFromOtherZone(Arrays.asList("a"),
                () -> { throw new IllegalStateException("pool broke"); },
                (c, k) -> done(map()), p.noClient::incrementAndGet, p.retried::add, p.attempts::incrementAndGet,
                t -> p.failures.incrementAndGet(), () -> false));
        assertEquals(p.fetches.get(), 0);
        assertEquals(p.failures.get(), 1); // visible as a failure, not mistaken for "nothing to do"
        assertEquals(p.noClient.get(), 0);
    }

    @Test(expectedExceptions = OutOfMemoryError.class)
    public void anErrorWhilePickingTheClientIsNotSwallowed() {
        SkippedKeyRetry.fetchFromOtherZone(Arrays.asList("a"),
                () -> { throw new OutOfMemoryError("x"); },
                (c, k) -> CompletableFuture.<Map<String, String>>completedFuture(map()), () -> { }, n -> { }, () -> { }, t -> { }, () -> false);
    }

    @Test
    public void throttlingStopsTheFetchBeforeItStarts() throws Exception {
        Probe p = new Probe();
        CompletableFuture<Map<String, String>> result = startFetch(p, Arrays.asList("a"), Arrays.asList(mock(EVCacheClient.class)),
                (c, k) -> done(map("a", "1")), true);
        assertEquals(p.fetches.get(), 0);
        assertEquals(p.attempts.get(), 0);
        assertTrue(p.retried.isEmpty()); // nothing was fetched, so nothing is counted as retried
        assertEquals(p.picks.get(), 0);  // and no client was picked, so the pool's round-robin did not advance
        // null means "nothing was attempted", so the caller's full retry still covers the skipped keys too
        assertNull(result);
    }

    @Test
    public void aThrowingOnAttemptIsTreatedAsAFailedAttemptAndKeepsWhatWasMerged() throws Exception {
        EVCacheClient broken = mock(EVCacheClient.class);
        EVCacheClient working = mock(EVCacheClient.class);
        Map<EVCacheClient, Map<String, String>> data = new HashMap<>();
        data.put(working, map("version", "v"));
        AtomicInteger attempts = new AtomicInteger();
        List<Throwable> failures = new ArrayList<>();

        Map<String, String> result = SkippedKeyRetry.retryMissing(map("item", "i"), KEYS, Arrays.asList(broken, working),
                fetchFrom(data, new HashMap<>()),
                () -> { if (attempts.incrementAndGet() == 1) throw new IllegalStateException("counter broke"); },
                failures::add, () -> false).get();

        assertEquals(failures.size(), 1);
        assertEquals(result.get("version"), "v");
        assertEquals(result.get("item"), "i");
    }

    @Test
    public void aFullRetryThatFindsNothingKeepsTheRecoveredKeys() throws Exception {
        // null from the full retry (no client, exhausted) with skipped keys recovered: keep those, do not return null
        assertEquals(SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true,
                () -> CompletableFuture.<Map<String, String>>completedFuture(null), n -> { }, t -> { }).get(), map("version", "v"));
        assertEquals(SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true,
                () -> done(new HashMap<String, String>()), n -> { }, t -> { }).get(), map("version", "v"));
    }

    @Test
    public void aFullRetryFailureWithRecoveredKeysIsReportedAndTheKeysAreKept() throws Exception {
        List<Throwable> reported = new ArrayList<>();
        Map<String, String> result = SkippedKeyRetry.combine(new HashMap<String, String>(), map("version", "v"), true, () -> {
            CompletableFuture<Map<String, String>> failed = new CompletableFuture<>();
            failed.completeExceptionally(new RuntimeException("x"));
            return failed;
        }, n -> { }, reported::add).get();
        assertEquals(result, map("version", "v"));
        assertEquals(reported.size(), 1);
    }
}
