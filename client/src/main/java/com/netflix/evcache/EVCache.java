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
 * <blockquote><pre>EVCache myCache =  new EVCache.Builder().setAppName("EVCACHE").setCacheName("Test").setDefaultTTL(3600).build();</pre></blockquote>
 * 
 * Below is an example to set value="John Doe" for key="name"
 * <blockquote><pre>
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
	 * Append to an existing data in the EVCACHE using the given EVCacheTranscoder.
	 *
	 * @param <T>
	 * @param key the key to which the value will be appended
	 * @param value the value that will be appended
	 * @param tc the EVCacheTranscoder to serialize the data
	 * @return a future indicating if the operation was success or not
	 * @throws IllegalStateException in the rare circumstance where queue
	 *         is too full to accept any more requests
	 */
	//<T> Future<Boolean>[] append(String key, T value, EVCacheTranscoder<T> tc) throws EVCacheException;

	/**
     * Set an object in the EVCACHE (using the default EVCacheTranscoder) regardless of any existing value.
     * 
     * The <code>timeToLive</code> value passed to memcached is as specified in the defaultTTL value for this cache
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.  
     * @param T the object to store
     * @return Array of futures representing the processing of this operation across all replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues with Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value) throws EVCacheException;

    /**
     * Set an object in the EVCACHE (using the default EVCacheTranscoder) regardless of any existing value.
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
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T the object to store
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, int timeToLive) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given EVCacheTranscoder regardless of any existing value.
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
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T the object to store
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc) throws EVCacheException;


    /**
     * Set an object in the EVCACHE using the given EVCacheTranscoder. If a value already exists it will be overwritten.
     * The value will expire after the given timeToLive in seconds. If the timeToLive exceeds 30 * 24 * 60 * 60 (seconds in 30 days)
     * then the timeToLive is considered an EPOC time which is number of seconds since 1/1/1970 
     *
     * @param key the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T the object to store
     * @param tc the EVCacheTranscoder to serialize the data
     * @param timeToLive the expiration of this object i.e. less than 30 days in seconds or the exact expire time as UNIX time
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues Serializing the value or any IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc, int timeToLive) throws EVCacheException;

    /**
     * Remove a current key value relation from the Cache.
     *
     * @param key the non-null key corresponding to the relation to be removed. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or any IO Related issues
     */
    Future<Boolean>[] delete(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given key. 
     *
     * @param key key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is none).  
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     *          
     * Note: If the data is replicated by zone, then we can the value from the zone local to the client. 
     *       If we cannot find this value then null is returned. This is transparent to the users. 
     */
    <T> T get(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given a key using the specified EVCacheTranscoder for deserialization.
     *
     * @param key key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is none).  
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     *          
     * Note: If the data is replicated by zone, then we can the value from the zone local to the client. 
     *       If we cannot find this value then null is returned. This is transparent to the users. 
     */
    <T> T get(String key, EVCacheTranscoder<T> tc) throws EVCacheException;

    
    /**
     * Get the value for the given single key and reset its expiration to the given timeToLive value.
     * i.e. if the value for a given key was supposed to expire in 100 seconds and this method is called with ttl set to 250
     * then the new expiry time for the given key will be 250 seconds. This is like calling touch for the given key with 250 seconds. 
     *
     * @param key the key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> T getAndTouch(String key, int timeToLive) throws EVCacheException;

    /**
     * Get with a single key and reset its expiration.
     *
     * @param key the key to get. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param timeToLive the new expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @param tc the EVCacheTranscoder to deserialize the data
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> T getAndTouch(String key, int timeToLive, EVCacheTranscoder<T> tc) throws EVCacheException;
    
    
    /**
     * Retrieve the value of a set of keys.
     *
     * @param keys the keys for which we need the values
     * @return a map of the values (for each value that exists). If the value of the given key does not exist then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String,T> getBulk(String... keys) throws EVCacheException;

    /**
     * Retrieve the value for a set of keys, using a specified EVCacheTranscoder for deserialization.
     *
     * @param keys keys to which we need the values
     * @param tc the EVCacheTranscoder to use for deserialization
     * @return a map of the values (for each value that exists). If the value of the given key does not exist then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String,T> getBulk(EVCacheTranscoder<T> tc, String... keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the default EVCacheTranscoder for deserialization.
     *
     * @param keys The collection of keys for which we need the values
     * @return a map of the values (for each value that exists). If the value of the given key does not exist then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String,T> getBulk(Collection<String> keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified EVCacheTranscoder for deserialization.
     *
     * @param keys The collection of keys for which we need the values
     * @param tc the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the value of the given key does not exist then null is returned
     * @throws EVCacheException in the rare circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or any IO Related issues
     */
    <T> Map<String,T> getBulk(Collection<String> keys, EVCacheTranscoder<T> tc) throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the default transcoder.
     *
     * @param key the key for which we need the value. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @return the Futures containing the Value 
     * @throws EVCacheException in the circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or timeout retrieving the value or any IO Related issues
     */
    <T> Future<T> getAsynchronous(String key) throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the given transcoder.
     *
     * @param key the key for which we need the value. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param tc the transcoder to use for deserialization
     * @throws EVCacheException in the circumstance where queue is too full to accept any more requests or 
     *          issues during deserialization or timeout retrieving the value or any IO Related issues
     */
    <T> Future<T> getAsynchronous(String key, EVCacheTranscoder<T> tc) throws EVCacheException;


    /**
     * A Builder that builds an EVCache based on the specified App Name, cache Name, TTl and EVCacheTranscoder. 
     * 
     * @author smadappa
     */
    class Builder {
        protected String _appName;
        protected String _cacheName;
        protected int _ttl = 900;
        protected EVCacheTranscoder<?> _transcoder;
        private boolean _zoneFallback = false;

        public Builder() { }
        
        public Builder setAppName(String appName) {
            if (appName == null) throw new IllegalArgumentException("param appName cannot be null.");
            this._appName = appName.toUpperCase();
            return this;
        }

        public Builder setCacheName(String cacheName) {
            if (cacheName == null) throw new IllegalArgumentException("param cacheName cannot be null.");
            this._cacheName = cacheName;
            return this;
        }

        public Builder setDefaultTTL(int ttl) {
            if (ttl < 0) throw new IllegalArgumentException("Time to Live cannot be less than 0.");        
            this._ttl= ttl;
            return this;
        }
        
        public <T> Builder setTranscoder(EVCacheTranscoder<T> transcoder) {
            this._transcoder = transcoder;
            return this;
        }
        
        public <T> Builder enableZoneFallback() {
            this._zoneFallback = true;
            return this;
        }

        public EVCache build() {
            if (_appName == null) throw new IllegalArgumentException("param appName cannot be null.");
            if (_cacheName == null) throw new IllegalArgumentException("param cacheName cannot be null.");
            final EVCacheImpl cache = new EVCacheImpl(_appName, _cacheName, _ttl, _transcoder, _zoneFallback);
            return cache;
        }
    }
}