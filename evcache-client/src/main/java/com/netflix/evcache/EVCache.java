package com.netflix.evcache;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.evcache.pool.EVCacheClientPoolManager;

import net.spy.memcached.transcoders.Transcoder;
import rx.Scheduler;
import rx.Single;

/**
 * An abstract interface for interacting with an Ephemeral Volatile Cache.
 *
 * <h3>Example</h3>
 * <p>
 * To create an instance of EVCache with AppName="EVCACHE", cachePrefix="Test"
 * and DefaultTTL="3600"
 * 
 * <b>Dependency Injection (Guice) Approach</b> <blockquote>
 * 
 * <pre>
 * {@literal @}Inject
 * public MyClass(EVCache.Builder builder,....) {
 *      EVCache myCache =  builder.setAppName("EVCACHE").setCachePrefix("Test").setDefaultTTL(3600).build();
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * Below is an example to set value="John Doe" for key="name" <blockquote>
 * 
 * <pre>
 * myCache.set("name", "John Doe");
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 * To read the value for key="name" <blockquote>
 * 
 * <pre>
 * String value = myCache.get("name");
 * </pre>
 * 
 * </blockquote>
 * 
 * </p>
 * 
 * @author smadappa
 */
public interface EVCache {

    public static enum Call {
        GET, GETL, GET_AND_TOUCH, ASYNC_GET, BULK, SET, DELETE, INCR, DECR, TOUCH, APPEND, PREPEND, REPLACE, ADD
    };

    /**
     * Set an object in the EVCACHE (using the default Transcoder) regardless of
     * any existing value.
     * 
     * The <code>timeToLive</code> value passed to memcached is as specified in
     * the defaultTTL value for this cache
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to store
     * @return Array of futures representing the processing of this operation
     *         across all replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues with Serializing the value or any
     *             IO Related issues
     */
    <T> Future<Boolean>[] set(String key, T value) throws EVCacheException;

    /**
     * Set an object in the EVCACHE (using the default Transcoder) regardless of
     * any existing value.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC
     * time (number of seconds since January 1, 1970, as a 32-bit int value), or
     * a number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, int timeToLive) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any
     * existing value.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC
     * time (number of seconds since January 1, 1970, as a 32-bit int value), or
     * a number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to store
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, Transcoder<T> tc) throws EVCacheException;



    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value using the default TTL and Transcoder.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T
     *            the object to store
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, EVCacheLatch.Policy policy) throws EVCacheException;


    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value with the given TTL.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time a.k.a EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, int timeToLive, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any existing value using the given Transcoder.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time (number of seconds since January 1, 1970, as a 32-bit int value), or a number of seconds starting from current time. In the latter case, this number of seconds may not exceed 60*60*24*30 (number of seconds in 30 days); if the number sent by a client is larger than that, the server will consider it to be real Unix time value rather than an offset from current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the key is properly encoded and does not contain whitespace or control characters.
     * @param T
     *            the object to store
     * @param tc
     *            the Transcoder to serialize the data
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can then be used to await until the count down has reached to 0 or the specified time has elapsed.
     * @return Array of futures representing the processing of this operation across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept any more requests or issues Serializing the value or any IO Related issues
     */
    <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any
     * existing value.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to store
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy)
            throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the default Transcoder &
     * default TTL. If the object does not exist in EVCACHE then the value is
     * not replaced.
     *
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to replace
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     * 
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     * 
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the given Transcoder &
     * default TTL. If the object does not exist in EVCACHE then the value is
     * not replaced.
     * 
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to replace
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     * 
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     * 
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, EVCacheLatch.Policy policy) throws EVCacheException;

    /**
     * Replace an existing object in the EVCACHE using the given Transcoder. If
     * the object does not exist in EVCACHE then the value is not replaced.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be replaced. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to replace
     * @param tc
     *            the Transcoder to serialize the data
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param policy
     *            The Latch will be returned based on the Policy. The Latch can
     *            then be used to await until the count down has reached to 0 or
     *            the specified time has elapsed.
     * 
     * @return EVCacheLatch which will encompasses the Operation. You can block
     *         on the Operation based on the policy to ensure the required
     *         criteria is met. The Latch can also be queried to get details on
     *         status of the operations
     * 
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, int timeToLive, EVCacheLatch.Policy policy)
            throws EVCacheException;

    /**
     * Set an object in the EVCACHE using the given Transcoder regardless of any
     * existing value.
     * 
     * The <code>timeToLive</code> value is passed to memcached exactly as
     * given, and will be processed per the memcached protocol specification:
     *
     * <blockquote> The actual value sent may either be Unix time aka EPOC time
     * (number of seconds since January 1, 1970, as a 32-bit int value), or a
     * number of seconds starting from current time. In the latter case, this
     * number of seconds may not exceed 60*60*24*30 (number of seconds in 30
     * days); if the number sent by a client is larger than that, the server
     * will consider it to be real Unix time value rather than an offset from
     * current time. </blockquote>
     *
     * @param key
     *            the key under which this object should be added. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the object to store
     * @param timeToLive
     *            the expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] set(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException;

    /**
     * Remove a current key value relation from the Cache.
     *
     * @param key
     *            the non-null key corresponding to the relation to be removed.
     *            Ensure the key is properly encoded and does not contain
     *            whitespace or control characters.
     * @return Array of futures representing the processing of this operation
     *         across all the replicas. If the future returns true then the key
     *         was deleted from Cache, if false then the key was not found thus
     *         not deleted. Note: In effect the outcome was what was desired.
     *         Note: If the null is returned then the operation timed out and
     *         probably the key was not deleted. In such scenario retry the
     *         operation.
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or any IO Related issues
     */
    Future<Boolean>[] delete(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given key.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> T get(String key) throws EVCacheException;

    /**
     * Retrieve the value for the given key.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @param scheduler
     *            the {@link Scheduler} to perform subscription actions on
     * @return the Value for the given key from the cache (null if there is
     *         none).
     */
    <T> Single<T> get(String key, Scheduler scheduler);

    /**
     * Retrieve the value for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the Value for the given key from the cache (null if there is
     *         none).
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     *
     *             Note: If the data is replicated by zone, then we can the
     *             value from the zone local to the client. If we cannot find
     *             this value then null is returned. This is transparent to the
     *             users.
     */
    <T> T get(String key, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value for the given a key using the specified Transcoder for
     * deserialization.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @param tc
     *            the Transcoder to deserialize the data
     * @param scheduler
     *            the {@link Scheduler} to perform subscription actions on
     * @return the Value for the given key from the cache (null if there is
     *         none).
     */
    <T> Single<T> get(String key, Transcoder<T> tc, Scheduler scheduler);

    /**
     * Retrieve the value for the given a key using the default Transcoder for
     * deserialization and reset its expiration using the passed timeToLive.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param scheduler
     *            the {@link Scheduler} to perform subscription actions on
     * @return the Value for the given key from the cache (null if there is
     *         none).
     */
    <T> Single<T> getAndTouch(String key, int timeToLive, Scheduler scheduler);
    
    /**
     * Retrieve the value for the given a key using the default Transcoder for
     * deserialization and reset its expiration using the passed timeToLive.
     *
     * @param key
     *            key to get. Ensure the key is properly encoded and does not
     *            contain whitespace or control characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param tc
     *            the Transcoder to deserialize the data
     * @param scheduler
     *            the {@link Scheduler} to perform subscription actions on
     * @return the Value for the given key from the cache (null if there is
     *         none).
     */
    <T> Single<T> getAndTouch(String key, int timeToLive, Transcoder<T> tc, Scheduler scheduler);
    
    
    
    /**
     * Get with a single key and reset its expiration.
     *
     * @param key
     *            the key to get. Ensure the key is properly encoded and does
     *            not contain whitespace or control characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> T getAndTouch(String key, int timeToLive) throws EVCacheException;

    /**
     * Get with a single key and reset its expiration.
     *
     * @param key
     *            the key to get. Ensure the key is properly encoded and does
     *            not contain whitespace or control characters.
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @param tc
     *            the Transcoder to deserialize the data
     * @return the result from the cache (null if there is none)
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> T getAndTouch(String key, int timeToLive, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value of a set of keys.
     *
     * @param keys
     *            the keys for which we need the values
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(String... keys) throws EVCacheException;

    /**
     * Retrieve the value for a set of keys, using a specified Transcoder for
     * deserialization.
     *
     * @param keys
     *            keys to which we need the values
     * @param tc
     *            the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the default
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values
     * @param tc
     *            the transcoder to use for deserialization
     * @return a map of the values (for each value that exists). If the Returned
     *         map contains the key but the value in null then the key does not
     *         exist in the cache. if a key is missing then we were not able to
     *         retrieve the data for that key due to some exception
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc) throws EVCacheException;

    /**
     * Retrieve the value for the collection of keys, using the specified
     * Transcoder for deserialization.
     *
     * @param keys
     *            The collection of keys for which we need the values
     * @param tc
     *            the transcoder to use for deserialization
     * @param timeToLive
     *            the new expiration of this object i.e. less than 30 days in
     *            seconds or the exact expiry time as UNIX time
     * @return a map of the values (for each value that exists). If the value of
     *         the given key does not exist then null is returned. Only the keys
     *         whose value are not null and exist in the returned map are set to
     *         the new TTL as specified in timeToLive.
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues during deserialization or any IO
     *             Related issues
     */
    <T> Map<String, T> getBulkAndTouch(Collection<String> keys, Transcoder<T> tc, int timeToLive)
            throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the
     * default transcoder.
     *
     * @param key
     *            the key for which we need the value. Ensure the key is
     *            properly encoded and does not contain whitespace or control
     *            characters.
     * @return the Futures containing the Value or null.
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues during deserialization or timeout
     *             retrieving the value or any IO Related issues
     *             
     * @deprecated This is a sub-optimal operation does not support Retries, Fast Failures, FIT, GC Detection, etc.
     *             Will be removed in a subsequent release 
     */
    <T> Future<T> getAsynchronous(String key) throws EVCacheException;

    /**
     * Get the value for given key asynchronously and deserialize it with the
     * given transcoder.
     *
     * @param key
     *            the key for which we need the value. Ensure the key is
     *            properly encoded and does not contain whitespace or control
     *            characters.
     * @param tc
     *            the transcoder to use for deserialization
     * @return the Futures containing the Value or null.
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues during deserialization or timeout
     *             retrieving the value or any IO Related issues
     *             
     * @deprecated This is a sub-optimal operation does not support Retries, Fast Failures, FIT, GC Detection, etc.
     *             Will be removed in a subsequent release 
     */
    <T> Future<T> getAsynchronous(String key, Transcoder<T> tc) throws EVCacheException;

    /**
     * Increment the given counter, returning the new value.
     *
     * @param key
     *            the key
     * @param by
     *            the amount to increment
     * @param def
     *            the default value (if the counter does not exist)
     * @param exp
     *            the expiration of this object
     * @return the new value, or -1 if we were unable to increment or add
     * @throws EVCacheException
     *             in the circumstance where timeout is exceeded or queue is
     *             full
     * 
     */
    public long incr(String key, long by, long def, int exp) throws EVCacheException;

    /**
     * Decrement the given counter, returning the new value.
     *
     * @param key
     *            the key
     * @param by
     *            the amount to decrement
     * @param def
     *            the default value (if the counter does not exist)
     * @param exp
     *            the expiration of this object
     * @return the new value, or -1 if we were unable to decrement or add
     * @throws EVCacheException
     *             in the circumstance where timeout is exceeded or queue is
     *             full
     * 
     */
    public long decr(String key, long by, long def, int exp) throws EVCacheException;

    /**
     * Append the given value to the existing value in EVCache. You cannot
     * append if the key does not exist in EVCache. If the value has not changed
     * then false will be returned.
     *
     * @param key
     *            the key under which this object should be appended. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the value to be appended
     * @param tc
     *            the transcoder the will be used for serialization
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] append(String key, T value, Transcoder<T> tc) throws EVCacheException;

    /**
     * Append the given value to the existing value in EVCache. You cannot
     * append if the key does not exist in EVCache. If the value has not changed
     * or does not exist then false will be returned.
     *
     * @param key
     *            the key under which this object should be appended. Ensure the
     *            key is properly encoded and does not contain whitespace or
     *            control characters.
     * @param T
     *            the value to be appended
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the circumstance where queue is too full to accept any
     *             more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] append(String key, T value) throws EVCacheException;

    /**
     * Touch the given key and reset its expiration time.
     *
     * @param key
     *            the key to touch
     * @param ttl
     *            the new expiration time in seconds
     * 
     * @return Array of futures representing the processing of this operation
     *         across all the replicas
     * @throws EVCacheException
     *             in the rare circumstance where queue is too full to accept
     *             any more requests or issues Serializing the value or any IO
     *             Related issues
     */
    <T> Future<Boolean>[] touch(String key, int ttl) throws EVCacheException;

    /**
     * The {@code appName} that will be used by this {@code EVCache}.
     * 
     * @param The
     *            name of the EVCache App cluster.
     * @return this {@code Builder} object
     */
    String getAppName();
    
    
    /**
     * The {@code cachePrefix} that will be used by this {@code EVCache}.
     * 
     * @param The
     *            name of the EVCache App cluster.
     * @return this {@code Builder} object
     */
    String getCachePrefix();
    
    /**
     * A Builder that builds an EVCache based on the specified App Name, cache
     * Name, TTl and Transcoder.
     * 
     * @author smadappa
     */
    public class Builder {
        private static final Logger log = LoggerFactory.getLogger(EVCacheImpl.class);
        private String _appName;
        private String _cachePrefix = null;
        private int _ttl = 900;
        private Transcoder<?> _transcoder = null;
        private boolean _serverGroupRetry = true;
        private boolean _enableExceptionThrowing = false;

        @Inject
        private EVCacheClientPoolManager _poolManager;

        public Builder() {
        }

        /**
         * The {@code appName} that will be used by this {@code EVCache}.
         * 
         * @param The
         *            name of the EVCache App cluster.
         * @return this {@code Builder} object
         */
        public Builder setAppName(String appName) {
            if (appName == null) throw new IllegalArgumentException("param appName cannot be null.");
            this._appName = appName.toUpperCase(Locale.US);
            if (!_appName.startsWith("EVCACHE")) log.warn("Make sure the app you are connecting to is EVCache App");
            return this;
        }

        /**
         * Adds {@code cachePrefix} to the key. This ensures there are no cache
         * collisions if the same EVCache app is used across multiple use cases.
         * If the cache is not shared we recommend to set this to
         * <code>null</code>. Default is <code>null</code>.
         *
         * @param cacheName.
         *            The cache prefix cannot contain colon (':') in it.
         * @return this {@code Builder} object
         */
        public Builder setCachePrefix(String cachePrefix) {
            this._cachePrefix = cachePrefix;
            if (_cachePrefix != null && _cachePrefix.indexOf(':') != -1) throw new IllegalArgumentException(
                    "param cacheName cannot contain ':' character.");
            return this;
        }

        /**
         * @deprecated Please use {@link #setCachePrefix(String)}
         * @see #setCachePrefix(String)
         * 
         *      Adds {@code cacheName} to the key. This ensures there are no
         *      cache collisions if the same EVCache app is used for across
         *      multiple use cases.
         *
         * @param cacheName
         * @return this {@code Builder} object
         */
        public Builder setCacheName(String cacheName) {
            return setCachePrefix(cacheName);
        }

        /**
         * The default Time To Live (TTL) for items in {@link EVCache} in
         * seconds. You can override the value by passing the desired TTL with
         * {@link EVCache#set(String, Object, int)} operations.
         *
         * @param ttl.
         *            Default is 900 seconds.
         * @return this {@code Builder} object
         */
        public Builder setDefaultTTL(int ttl) {
            if (ttl < 0) throw new IllegalArgumentException("Time to Live cannot be less than 0.");
            this._ttl = ttl;
            return this;
        }

        /**
         * The default {@link Transcoder} to be used for serializing and
         * de-serializing items in {@link EVCache}.
         *
         * @param transcoder
         * @return this {@code Builder} object
         */
        public <T> Builder setTranscoder(Transcoder<T> transcoder) {
            this._transcoder = transcoder;
            return this;
        }

        /**
         * @deprecated Please use {@link #enableRetry()}
         * 
         *             Will enable retries across Zone (Server Group).
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableZoneFallback() {
            this._serverGroupRetry = true;
            return this;
        }

        /**
         * Will enable retry across Server Group for cache misses and exceptions
         * if there are multiple Server Groups for the given EVCache App and
         * data is replicated across them. This ensures the Hit Rate continues
         * to be unaffected whenever a server group loses instances.
         * 
         * By Default retry is enabled.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableRetry() {
            this._serverGroupRetry = true;
            return this;
        }

        /**
         * Will disable retry across Server Groups. This means if the data is
         * not found in one server group null is returned.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder disableRetry() {
            this._serverGroupRetry = true;
            return this;
        }

        /**
         * @deprecated Please use {@link #disableRetry()}
         * 
         *             Will disable retry across Zone (Server Group).
         *
         * @return this {@code Builder} object
         */
        public <T> Builder disableZoneFallback() {
            this._serverGroupRetry = false;
            return this;
        }

        /**
         * By Default exceptions are not propagated and null values are
         * returned. By enabling exception propagation we return the
         * {@link EVCacheException} whenever the operations experience them.
         *
         * @return this {@code Builder} object
         */
        public <T> Builder enableExceptionPropagation() {
            this._enableExceptionThrowing = true;
            return this;
        }

        /**
         * Returns a newly created {@code EVCache} based on the contents of the
         * {@code Builder}.
         */
        @SuppressWarnings("deprecation")
        public EVCache build() {
            if (_poolManager == null) {
                _poolManager = EVCacheClientPoolManager.getInstance();
                if (log.isDebugEnabled()) log.debug("_poolManager - " + _poolManager + " through getInstance");
            }
            if (_appName == null) throw new IllegalArgumentException("param appName cannot be null.");
            final EVCacheImpl cache = new EVCacheImpl(_appName, _cachePrefix, _ttl, _transcoder, _serverGroupRetry,
                    _enableExceptionThrowing, _poolManager);
            return cache;
        }
    }
}
