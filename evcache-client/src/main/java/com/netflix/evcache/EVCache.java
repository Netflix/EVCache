package com.netflix.evcache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;


/**
 * An abstract interface for interacting with Ephemeral Volatile Caches.
 *
 * <h3>Example</h3>
 * <p>
 * To create an instance of EVCache with AppName="EVCACHE", cacheName="Test" and DefaultTTL="3600"
 * <blockquote><pre>
 * EVCache myCache =  new EVCache.Builder().setAppName("EVCACHE").setCacheName("Test").setDefaultTTL(3600).build();
 * </pre></blockquote>
 * Below is an example to set value="John Doe" for key="name"
 * <blockquote><pre>
 *
 *  myCache.set("name", "John Doe");
 * </pre></blockquote>
 *
 *
 * To read the value for key="name"
 * <blockquote><pre>
 * String value = myCache.get("name");
 * </pre></blockquote>
 *
 * </p>
 * @author smadappa
 */
public interface EVCache {

    /**
     * Set an object in the EVCACHE (using the default {@link EVCacheTranscoder}) regardless of any existing value.
     *
     * The <code>timeToLive</code> value passed to memcached is as specified in the defaultTTL value for this cache
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does
     *          not contain whitespace or control characters.
     * @param value the object to store
     * @return Array of futures representing the processing of this operation across all replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues with Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value) throws EVCacheException;

    /**
     * Set an object in the EVCACHE (using the default {@link EVCacheTranscoder}) regardless of any existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote>
     * The actual value sent may either be
     * Unix time aka EPOCH time (number of seconds since January 1, 1970, as a 32-bit int
     * value), or a number of seconds starting from current time. In the
     * latter case, this number of seconds may not exceed 60*60*24*30 (number
     * of seconds in 30 days); if the number sent by a client is larger than
     * that, the server will consider it to be real Unix time value rather
     * than an offset from current time.
     * </blockquote>
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded
     *        and does not contain whitespace or control characters.
     * @param value the object to store
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds or the exact
     *          expiry time as UNIX time
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, int timeToLive) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given {@link EVCacheTranscoder} regardless of any existing value.
     *
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote>
     * The actual value sent may either be
     * Unix time aka EPOC time (number of seconds since January 1, 1970, as a 32-bit int
     * value), or a number of seconds starting from current time. In the
     * latter case, this number of seconds may not exceed 60*60*24*30 (number
     * of seconds in 30 days); if the number sent by a client is larger than
     * that, the server will consider it to be real Unix time value rather
     * than an offset from current time.
     * </blockquote>
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does
     *          not contain whitespace or control characters.
     * @param value the object to store
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc) throws EVCacheException;


    /**
     * Set an object in the EVCACHE using the given {@link EVCacheTranscoder}.
     * If a value already exists it will be overwritten.
     * The value will expire after the given timeToLive in seconds.
     * If the timeToLive exceeds 30 * 24 * 60 * 60 (seconds in 30 days)
     * then the timeToLive is considered an EPOC time which is number of seconds since 1/1/1970
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does
     *          not contain whitespace or control characters.
     * @param value the object to store
     * @param tc the {@link EVCacheTranscoder} to serialize the data
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds
     *          or the exact expire time as UNIX time
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests
     *          or issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc, int timeToLive) throws EVCacheException;

    /**
     * Remove a current key value relation from the Cache.
     *
     * @param key the non-null key corresponding to the relation to be removed. Ensure the key is properly encoded and
     *           does not contain whitespace or control characters.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests
     *          or any IO Related issues
     */
    Future<Boolean>[] delete(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given key.
     *
     * @param key key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is none).
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests
     *          or issues during deserialization or any IO Related issues
     *
     * Note: If the data is replicated by zone, then we can the value from the zone local to the client.
     *       If we cannot find this value then null is returned. This is transparent to the users.
     */
    <T> T get(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given a key using the specified {@link EVCacheTranscoder} for deserialization.
     *
     * @param key key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is none).
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     * Note: If the data is replicated by zone, then we can the value from the zone local to the client.
     *       If we cannot find this value then null is returned. This is transparent to the users.
     */
    <T> T get(String key, EVCacheTranscoder<T> tc) throws EVCacheException;


    /**
     * Get the value for the given single key and reset its expiration to the given timeToLive value.
     * i.e. if the value for a given key was supposed to expire in 100 seconds and this method is called with TTL
     * set to 250 then the new expiry time for the given key will be 250 seconds. This is like calling touch for
     * the given key with 250 seconds.
     *
     * @param key the key to get. Ensure the key is properly encoded and does not contain whitespace
     *          or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds or the exact
     *          expiry time as UNIX time
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests
     *           or issues during deserialization or any IO Related issues
     */
    <T> T getAndTouch(String key, int timeToLive) throws EVCacheException;

    /**
     * Get with a single key and reset its expiration.
     *
     * @param key the key to get. Ensure the key is properly encoded and does not contain whitespace
     *          or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds
     *           or the exact expiry time as UNIX time
     * @param tc the {@link EVCacheTranscoder} to deserialize the data
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> T getAndTouch(String key, int timeToLive, EVCacheTranscoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value of a set of keys.
     *
     * @param keys the keys for which we need the values
     * @return a map of the values (for each value that exists). If the value of the given key does not exist
     *          then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String, T> getBulk(String... keys) throws EVCacheException;

    /**
     * Retrieve the value for a set of keys, using a specified {@link EVCacheTranscoder} for deserialization.
     *
     * @param keys keys to which we need the values
     * @param tc the {@link EVCacheTranscoder} to use for deserialization
     * @return a map of the values (for each value that exists). If the value of the given key does not exist
     *           then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String, T> getBulk(EVCacheTranscoder<T> tc, String... keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the default {@link EVCacheTranscoder} for deserialization.
     *
     * @param keys The collection of keys for which we need the values
     * @return a map of the values (for each value that exists). If the value of the given key does not exist
     *           then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified {@link EVCacheTranscoder} for deserialization.
     *
     * @param keys The collection of keys for which we need the values
     * @param tc the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the value of the given key does not exist
     *           then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys, EVCacheTranscoder<T> tc) throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the default transcoder.
     *
     * @param key the key for which we need the value. Ensure the key is properly encoded and
     *          does not contain whitespace or control characters.
     * @return the Futures containing the Value
     * @throws EVCacheException in the circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or timeout retrieving the value or any IO Related issues
     */
    <T> Future<T> getAsynchronous(String key) throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the given transcoder.
     *
     * @param key the key for which we need the value. Ensure the key is properly encoded and
     *           does not contain whitespace or control characters.
     * @param tc the transcoder to use for deserialization
     * @throws EVCacheException in the circumstance where queue is too full to accept any more requests or
     *          issues during deserialization or timeout retrieving the value or any IO Related issues
     */
    <T> Future<T> getAsynchronous(String key, EVCacheTranscoder<T> tc) throws EVCacheException;


    /**
     * A Builder that builds an EVCache based on the specified App Name, cache Name, TTl and {@link EVCacheTranscoder}.
     */
    class Builder {
        /**
         * The default TTL for the data that is stored in EVCache.
         */
        public static final int DEFAULT_TTL = 900;

        private String appName;
        private String cacheName;
        private int ttl = DEFAULT_TTL;
        private EVCacheTranscoder<?> transcoder;
        private boolean zoneFallback = false;

        /**
         * Creates an instance of Builder.
         */
        public Builder() { }

        /**
         * Converts the appName to uppercase and sets it in this object.
         * @param pAppName -  the name of the EVCache app
         * @return a reference to this object
         */
        public Builder setAppName(String pAppName) {
            if (pAppName == null) { throw new IllegalArgumentException("param appName cannot be null."); }
            this.appName = pAppName.toUpperCase();
            return this;
        }

        /**
         *  Sets the cacheName.
         * @param pCacheName the name of the cache. This value is prepended to the key so as to avoid any key collision.
         *                      This is optional and can be null in which case the key is left as is.
         * @return a reference to this object
         */
        public Builder setCacheName(String pCacheName) {
            //TODO check for contains ":"
            this.cacheName = pCacheName;
            return this;
        }

        /**
         * Sets the default TTL. If not set then DEFAULT_TTL value is used.
         * @param pttl - the Time to Live for this object in seconds if less than 30 days in seconds
         *         or the exact expire time as UNIX time
         * @return a reference to this object
         */
        public Builder setDefaultTTL(int pttl) {
            if (pttl < 0) { throw new IllegalArgumentException("Time to Live cannot be less than 0."); }
            this.ttl = pttl;
            return this;
        }

        /**
         * The Default {@link EVCacheTranscoder} to be used if one is not passed while performing various operations.
         * @param pTranscoder - The {@link EVCacheTranscoder} to be set. Can be null.
         * @return a reference to this object
         */
        public <T> Builder setTranscoder(EVCacheTranscoder<T> pTranscoder) {
            this.transcoder = pTranscoder;
            return this;
        }

        /**
         * Enables zone fallback. Default is false.
         * @return a reference to this object
         */
        public <T> Builder enableZoneFallback() {
            this.zoneFallback = true;
            return this;
        }

        /**
         * Creates an instance of {@link EVCache} based on the various parameters that were set.
         * @return an instance of {@link EVCache}
         */
        public EVCache build() {
            if (appName == null) { throw new IllegalArgumentException("param appName cannot be null."); }
            final EVCacheImpl cache = new EVCacheImpl(appName, cacheName, ttl, transcoder, zoneFallback);
            return cache;
        }
    }
}
