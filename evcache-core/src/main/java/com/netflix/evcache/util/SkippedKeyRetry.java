package com.netflix.evcache.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import com.netflix.evcache.pool.EVCacheClient;

/**
 * Retries the keys of an async bulk read that the client skipped (their node was inactive or its read queue was over
 * the threshold) against fallback clients in other server groups, merging what comes back into the result in hand.
 *
 * <p>When one zone is under read pressure, the client skips the keys that hash to its busy nodes and never sends them.
 * The async bulk path used to retry only when the whole result was empty, so those keys came back as plain misses.
 * Only skipped keys are retried: a key that was sent and genuinely missed is not looked up again elsewhere.
 *
 * <p>This class is kept free of {@code EVCacheImpl} state so the retry logic can be tested on its own.
 */
public final class SkippedKeyRetry {

    private SkippedKeyRetry() {
    }

    /**
     * @return a thread safe set to collect skipped keys into, or null when the retry is not in effect, which is
     *         unless zone fallback is on for bulk reads and the property enabling this retry is on. A null set means no
     *         per-call tracking is done at all.
     */
    public static <K> Set<K> newSkippedKeySet(boolean hasZoneFallback, boolean enabled) {
        return hasZoneFallback && enabled ? ConcurrentHashMap.<K>newKeySet() : null;
    }

    /**
     * Resolves the raw key strings the client skipped back to the keys they were built from. A raw string is looked
     * up as a plain key first, then as a hashed key; strings that match neither are ignored.
     */
    public static <K> void addSkippedKeys(Set<String> skippedRaw, Map<String, K> plainKeys, Map<String, K> hashedKeys,
                                          Set<K> skipped) {
        for (String raw : skippedRaw) {
            K key = plainKeys.get(raw);
            if (key == null) key = hashedKeys.get(raw);
            if (key != null) skipped.add(key);
        }
    }

    /**
     * @return the skipped keys, in request order, that are worth fetching from another zone right away, in parallel
     *         with the main call. Empty when nothing was skipped or when every key was skipped: in that case the main
     *         call returns an empty result immediately and the existing full retry takes over, so fetching here too
     *         would only duplicate it.
     */
    public static <K> List<K> keysToRetryEarly(Collection<K> allKeys, Set<K> skipped) {
        final List<K> retry = new ArrayList<>();
        if (skipped == null || skipped.isEmpty()) return retry;
        // unique keys in request order, so a key requested twice is neither fetched twice nor throws the count off
        final java.util.Set<K> unique = new java.util.LinkedHashSet<>(allKeys);
        for (K key : unique) {
            if (skipped.contains(key)) retry.add(key);
        }
        return retry.size() >= unique.size() ? new ArrayList<K>(0) : retry;
    }

    /**
     * @return the keys from {@code allKeys}, in order, that were not skipped, i.e. the ones that were actually sent
     */
    public static <K> List<K> sentKeys(Collection<K> allKeys, Set<K> skipped) {
        final List<K> sent = new ArrayList<>(allKeys.size());
        // unique keys in request order, like keysToRetryEarly
        for (K key : new java.util.LinkedHashSet<>(allKeys)) {
            if (skipped == null || !skipped.contains(key)) sent.add(key);
        }
        return sent;
    }

    /**
     * @return how many keys {@code recovered} has a value for that {@code found} has none for
     */
    public static <K, T> int countRecovered(Map<K, T> found, Map<K, T> recovered) {
        int n = 0;
        if (recovered != null) {
            for (Map.Entry<K, T> e : recovered.entrySet()) {
                if (e.getValue() != null && (found == null || found.get(e.getKey()) == null)) n++;
            }
        }
        return n;
    }

    /**
     * Starts the fetch of the skipped keys from one other zone, without waiting for anything: the fetch has been
     * started by the time this returns. Exactly one cross-zone call is made, to the one client {@code pickClient}
     * returns.
     *
     * @param retryKeys  the skipped keys to fetch, see {@link #keysToRetryEarly}
     * @param pickClient picks the one other zone's client, or returns null if there is none; called at most once
     * @param onNoClient told when there is no client to ask
     * @param onRetried  told how many keys are being fetched
     * @return a future of what was recovered which never fails except with an {@link Error}, or null when there is
     *         nothing to do or nothing could be started
     */
    public static <K, T> CompletableFuture<Map<K, T>> fetchFromOtherZone(
            List<K> retryKeys,
            Supplier<EVCacheClient> pickClient,
            BiFunction<EVCacheClient, List<K>, CompletableFuture<Map<K, T>>> fetch,
            Runnable onNoClient,
            LongConsumer onRetried,
            Runnable onAttempt,
            Consumer<Throwable> onFailure,
            BooleanSupplier shouldStop) {
        if (retryKeys.isEmpty()) return null;
        // An exception while setting up must not cost the caller the keys the main call returns. Errors are not caught.
        try {
            // one call, so the throttle decision is made once, here, before a client is picked (which advances the
            // pool's round-robin) and before anything is counted as retried
            if (shouldStop.getAsBoolean()) {
                // null, like having no client: nothing was attempted, so the full retry still covers every key
                return null;
            }
            final EVCacheClient client = pickClient.get();
            if (client == null) {
                onNoClient.run();
                return null;
            }
            onRetried.accept(retryKeys.size());
            return retryMissing(new HashMap<K, T>(), retryKeys, Collections.singletonList(client), fetch, onAttempt, onFailure, () -> false)
                    .exceptionally(t -> recoverFromFailure(t, new HashMap<K, T>()));
        } catch (Exception e) {
            // a setup failure (for example the pool throwing) must show up as a failure, not look like "nothing to do"
            try {
                onFailure.accept(e);
            } catch (Exception ignored) {
                // reporting must not fail the call either
            }
            return null;
        }
    }

    /**
     * Combines the main call's result with what the parallel fetch of skipped keys recovered. If the main call
     * returned nothing, the existing full retry (for the keys it sent) still runs, as it did before the skipped key
     * fetch existed, and its result is merged in as well.
     *
     * @param data             the main call's result
     * @param recovered        what the skipped key fetch recovered
     * @param fullRetryAllowed whether zone fallback is on for bulk reads
     * @param fullRetry        starts the full retry for the keys the main call sent
     * @param onRecovered      told how many keys the skipped key fetch recovered
     * @param onFullRetryFailure told when the full retry failed but the recovered keys are being returned anyway
     * @return the merged result, or null if nothing was found anywhere and the full retry itself found nothing
     */
    public static <K, T> CompletableFuture<Map<K, T>> combine(
            Map<K, T> data,
            Map<K, T> recovered,
            boolean fullRetryAllowed,
            Supplier<CompletableFuture<Map<K, T>>> fullRetry,
            LongConsumer onRecovered,
            Consumer<Throwable> onFullRetryFailure) {
        final Map<K, T> merged = merge(data, recovered);
        onRecovered.accept(countRecovered(data, recovered));
        if (!fullRetryAllowed || (data != null && !data.isEmpty())) {
            return CompletableFuture.completedFuture(merged);
        }
        CompletableFuture<Map<K, T>> full;
        try {
            full = fullRetry.get();
        } catch (Exception e) {
            // same rule as for a failed future below: recovered keys are kept, otherwise the failure propagates
            if (merged.isEmpty()) {
                final CompletableFuture<Map<K, T>> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return failed;
            }
            return CompletableFuture.completedFuture(merged);
        }
        if (full == null) return CompletableFuture.completedFuture(merged);
        return full.handle((f, ex) -> {
            if (ex != null) {
                // with nothing recovered the failure propagates as it always did; otherwise keep what we have
                if (merged.isEmpty()) {
                    throw ex instanceof java.util.concurrent.CompletionException
                            ? (java.util.concurrent.CompletionException) ex
                            : new java.util.concurrent.CompletionException(ex);
                }
                final Map<K, T> kept = recoverFromFailure(ex, merged); // an Error propagates here
                try {
                    onFullRetryFailure.accept(ex);
                } catch (Exception ignored) {
                    // reporting must not cost the recovered keys
                }
                return kept;
            }
            if (f == null && merged.isEmpty()) return null; // keeps the shape a failed full retry always had
            return merge(merged, f);
        });
    }

    /**
     * For use in {@code exceptionally}: a failed fetch of skipped keys must not fail the call, so it yields
     * {@code fallback}, but an {@link Error} (out of memory and the like) is never swallowed, wherever in the chain
     * it was thrown.
     */
    public static <R> R recoverFromFailure(Throwable t, R fallback) {
        Throwable cause = t;
        while ((cause instanceof java.util.concurrent.CompletionException || cause instanceof java.util.concurrent.ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof Error) throw (Error) cause;
        return fallback;
    }

    /**
     * @return a new map with everything in {@code found} plus the non-null entries of {@code recovered} for keys that
     *         {@code found} has no value for. Either argument may be null.
     */
    public static <K, T> Map<K, T> merge(Map<K, T> found, Map<K, T> recovered) {
        final Map<K, T> merged = found == null ? new HashMap<K, T>() : new HashMap<K, T>(found);
        if (recovered != null) {
            for (Map.Entry<K, T> e : recovered.entrySet()) {
                if (e.getValue() != null && merged.get(e.getKey()) == null) merged.put(e.getKey(), e.getValue());
            }
        }
        return merged;
    }

    private static <K, T> List<K> missingKeys(Collection<K> candidates, Map<K, T> found) {
        final List<K> missing = new ArrayList<>(candidates.size());
        for (K key : new java.util.LinkedHashSet<>(candidates)) { // unique, so a key is never asked for twice
            if (found.get(key) == null) {
                missing.add(key);
            }
        }
        return missing;
    }

    /**
     * Tries each fallback client in order, asking only for the candidate keys that are still missing, until every one
     * has a value or the clients are exhausted. Values already in {@code found} are never replaced. A fallback client
     * that fails (exception or failed future) or returns nothing is skipped, and the result already in hand is always
     * returned, never an error. {@link Error}s are not caught.
     *
     * <p>The clients are tried strictly one after another and the merged map is only touched from the single stage
     * that is running at that moment, so it is a plain {@code HashMap}. Do not change this to fetch from several
     * clients at once without making the accumulator thread safe. (A {@code ConcurrentHashMap} is not a drop-in
     * choice either: it rejects the null values that a result map may contain.)
     *
     * @param found           the partial result from the first attempt (not modified). A key with a null value counts
     *                        as having no value, the same as an absent key.
     * @param candidateKeys   the keys worth retrying, normally the result of {@link #keysToRetryEarly}. Only these
     *                        are ever asked for, so keys that were sent and missed are not looked up again.
     * @param fallbackClients clients to try, in order, normally the list the pool returns for the original group
     * @param fetch           reads the given keys from the given client
     * @param onAttempt       called once before each fallback client is tried
     * @param onFailure       called with the cause when a fallback call fails (exception or failed future); the call
     *                        is then skipped. A null result is not a failure.
     * @param shouldStop      checked before each attempt; when true no further client is tried and the merged result
     *                        is returned (used to honor request throttling)
     * @return a future holding the merged result
     */
    public static <K, T> CompletableFuture<Map<K, T>> retryMissing(
            Map<K, T> found,
            Collection<K> candidateKeys,
            List<EVCacheClient> fallbackClients,
            BiFunction<EVCacheClient, List<K>, CompletableFuture<Map<K, T>>> fetch,
            Runnable onAttempt,
            Consumer<Throwable> onFailure,
            BooleanSupplier shouldStop) {
        final Map<K, T> merged = found == null ? new HashMap<K, T>() : new HashMap<K, T>(found);
        return retryFrom(0, merged, candidateKeys, fallbackClients, fetch, onAttempt, onFailure, shouldStop);
    }

    private static <K, T> CompletableFuture<Map<K, T>> retryFrom(
            int index,
            Map<K, T> merged,
            Collection<K> candidateKeys,
            List<EVCacheClient> fallbackClients,
            BiFunction<EVCacheClient, List<K>, CompletableFuture<Map<K, T>>> fetch,
            Runnable onAttempt,
            Consumer<Throwable> onFailure,
            BooleanSupplier shouldStop) {
        final List<K> missing = missingKeys(candidateKeys, merged);
        if (missing.isEmpty() || index >= fallbackClients.size() || shouldStop.getAsBoolean()) {
            return CompletableFuture.completedFuture(merged);
        }

        CompletableFuture<Map<K, T>> attempt;
        try {
            onAttempt.run();
            attempt = fetch.apply(fallbackClients.get(index), missing);
        } catch (Exception e) {
            attempt = new CompletableFuture<>();
            attempt.completeExceptionally(e);
        }
        if (attempt == null) {
            attempt = CompletableFuture.completedFuture(null);
        }

        return attempt
                .handle((data, ex) -> {
                    if (ex != null) {
                        try {
                            onFailure.accept(ex);
                        } catch (Exception ignored) {
                            // reporting a failure must never discard what was already merged
                        }
                    } else if (data != null) {
                        for (Map.Entry<K, T> entry : data.entrySet()) {
                            // same rule as merge: a key with a null value has no value, so it can be filled
                            if (entry.getValue() != null && merged.get(entry.getKey()) == null) {
                                merged.put(entry.getKey(), entry.getValue());
                            }
                        }
                    }
                    return merged;
                })
                .thenCompose(m -> retryFrom(index + 1, m, candidateKeys, fallbackClients, fetch, onAttempt, onFailure, shouldStop));
    }
}
