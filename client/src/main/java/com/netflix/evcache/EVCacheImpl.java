package com.netflix.evcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.CASValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;

/**
 * An implementation of a Ephemeral Volatile Cache.
 */
@SuppressWarnings("unchecked")
public final class EVCacheImpl implements EVCache {

    private static final Logger log = LoggerFactory.getLogger(EVCacheImpl.class);


    /*
     * The name of the EVCache cluster.
     */
    private final String _appName;

    /*
     * The optional cache name that is used to prepend the key so as to avoid key collision when an
     * EVCache server is used to store multiple values for a key.
     *
     * <p>
     * For Example to store Date of Birth (DOB) & City for a john, it is stored as
     *       DOB:john=1/1/2000
     *       City:john=Campbell
     *
     * To store Date Of Birth the cache name is DOB and to store city the cache name is City.
     * </p>
     */
    private final String _cacheName;

    /*
     * The default transcoder that is to be used for serialization if one is not passed during the calls.
     */
    private final EVCacheTranscoder<?> _defaultTranscoder;

    /*
     * The flag to store if zone fallback is enabled.
     * Typically Zone fallback mode should be enabled when data is replicated across zones and
     *  it is faster to fetch data across zones than from the source.
     */
    private final boolean _zoneFallback;

    /*
     * The default time to live for the value if one is not passed while trying to set data in EVCache
     */
    private final int _timeToLive;

    /*
     * The pool that is used to perform all operations on EVCache.
     */
    private final EVCacheClientPool _pool;

    /*
     * If true then exceptions are returned else null is returned.
     */
    private final DynamicBooleanProperty _throwException;

    /*
     * If true then operations that fail (null value is considered a failure) are tried on other zone.
     * This is helpful especially during
     *      - instance/zone failures
     *  - cluster expansion in a zone
     *  - new zone being provisioned
     */
    private final DynamicBooleanProperty _zoneFallbackFP;

    private final Counter HIT_COUNTER, MISS_COUNTER, BULK_HIT_COUNTER, BULK_MISS_COUNTER;
    private final Counter NULL_CLIENT_COUNTER, FALLBACK_HIT_COUNTER, FALLBACK_MISS_COUNTER;

    /**
     * Creates an EVCache instance which performs all the operations on the store.
     *
     * @param appName - The name of the EVCache app
     * @param cacheName - The optional cache name that is used for namespacing when this EVCache app is used to store
     *                      various use cases. This can be null.
     * @param timeToLive -  The default TTL that is used when setting the values if one is not passed while setting the
     *                      values in EVCache.Negative values will cause exception to be thrown.
     *                      Zero causes the value to never expire. This value might be removed when EVCache runs out of
     *                      memory causing the value to be evicted based on LRU.
     * @param transcoder - The default transcoder that will be used for serialization if one is not passed during
     *                      the operations. If null then the providers default is used.
     * @param enableZoneFallback - If true then on cache misses the operations are tried on other zones.
     *                         By default all the operations are performed on the local zone.
     *                         If EVCache is not available in the local zone then a zone is picked randomly.
     */
    EVCacheImpl(String appName, String cacheName, int timeToLive, EVCacheTranscoder<?> transcoder, boolean enableZoneFallback) {
        this._appName = appName;
        this._cacheName = cacheName;
        this._timeToLive = timeToLive;
        this._defaultTranscoder = transcoder;
        this._zoneFallback = enableZoneFallback;

        final String _metricName = (cacheName == null) ? appName : appName + "." + cacheName;
        NULL_CLIENT_COUNTER = Monitors.newCounter(_metricName  + ":NULL_CLIENT");
        FALLBACK_HIT_COUNTER = Monitors.newCounter(_metricName  + ":FALLBACK:HIT");
        FALLBACK_MISS_COUNTER = Monitors.newCounter(_metricName  + ":FALLBACK:MISS");
        HIT_COUNTER = Monitors.newCounter(_metricName  + ":HIT");
        MISS_COUNTER = Monitors.newCounter(_metricName  + ":MISS");
        BULK_HIT_COUNTER = Monitors.newCounter(_metricName  + ":BULK:HIT");
        BULK_MISS_COUNTER = Monitors.newCounter(_metricName  + "BULK::MISS");


        this._pool = EVCacheClientPoolManager.getInstance().getEVCacheClientPool(appName);
        _throwException = DynamicPropertyFactory.getInstance().getBooleanProperty(_metricName + ".throw.exception", false);
        _zoneFallbackFP = DynamicPropertyFactory.getInstance().getBooleanProperty(_metricName + ".fallback.zone", true);
    }

    /**
     * Returns the canonicalized form of the given key by optionally prepending the cachename to the key.
     * if the cachename is null or of zero length then the key is returned as is.
     *
     * @param key - The key for which we need the canonicalized key
     * @return - the canonicalized key.
     */
    protected String getCanonicalizedKey(String key) {
        if (this._cacheName == null || _cacheName.length() == 0) {
            return key;
        }
        return _cacheName + ':' + key;
    }

    /**
     * Returns the actual key from the canonicalized key.
     *
     * @param canonicalizedKey - The canonicalized key
     * @return - the key removing the cache name from it if any.
     */
    protected String getKey(String canonicalizedKey) {
        if (canonicalizedKey == null) {
            return canonicalizedKey;
        }

        return (this._cacheName == null || _cacheName.length() == 0 || canonicalizedKey.indexOf(':') == -1)
                ? canonicalizedKey : canonicalizedKey.substring(canonicalizedKey.indexOf(':') + 1);
    }

    /**
     * if the pool does not support fallback then return false immediately.
     * If EVCache is in zone fallback mode then true is returned else false.
     * In zone fallback if an operation fails then it is tried on other zones.
     * Zone fallback can be enabled by setting the _zoneFallback to true while creating this instance.
     * Zone fallback can also be turned on dynamically by setting the below DynamicProperty to true
     *      <appname>.<cachename>.fallback.zone=true
     *
     * @return true if in zone fallback else false
     */
    protected boolean isInZoneFallback() {
        if (!_pool.supportsFallback()) {
            return false;
        }
        if (_zoneFallback) {
            return true;
        }
        return _zoneFallbackFP.get();
    }

    /**
     * {@inheritDoc}
     */
    public <T> T get(String key) throws EVCacheException {
        return this.get(key, (EVCacheTranscoder<T>) _defaultTranscoder);
    }


    /**
     * {@inheritDoc}
     */
    public <T> T get(String key, EVCacheTranscoder<T> tc) throws EVCacheException {
        if (null == key) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        final EVCacheClient client = _pool.getEVCacheClient();
        if (client == null) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Couldn't find an Client to get the data for key : " + key);
            }
            return null;  // Fast failure
        }

        try {
            final String canonicalKey = getCanonicalizedKey(key);
            T data = client.get(canonicalKey, tc);
            if (data == null && isInZoneFallback()) {
                final EVCacheClient fbClient = _pool.getEVCacheClientExcludeZone(client.getZone());
                if (fbClient == null) {
                    return null;
                }
                data = fbClient.get(canonicalKey, tc);
                if (data == null) {
                    FALLBACK_MISS_COUNTER.increment();
                } else {
                    FALLBACK_HIT_COUNTER.increment();
                }
            }
            if (data != null)  {
                HIT_COUNTER.increment();
            } else {
                MISS_COUNTER.increment();
            }
            if (log.isDebugEnabled()) {
                log.debug("GET : key [" + key + "], Value [" + data + "]");
            }
            return data;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception while getting data for key : " + key, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception getting data for key : " + key, ex);
        }
    }


    /**
     * {@inheritDoc}
     */
    public <T> T getAndTouch(String key, int timeToLive) throws EVCacheException {
        return this.getAndTouch(key, timeToLive, (EVCacheTranscoder<T>) _defaultTranscoder);
    }


    /**
     * {@inheritDoc}
     */
    public <T> T getAndTouch(String key, int timeToLive, EVCacheTranscoder<T> tc) throws EVCacheException {
        if (null == key) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        final EVCacheClient[] clients = _pool.getAllEVCacheClients();
        if (clients == null || clients.length == 0) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Could not find a client to set the data");
            }
            return null;  // Fast failure
        }

        try {
            final Future<CASValue<T>>[] futures = new Future[clients.length];
            final String canonicalKey = getCanonicalizedKey(key);
            int index = 0, timeout = 0;
            for (EVCacheClient client : clients) {
                futures[index++] = client.asyncGetAndTouch(canonicalKey, tc, timeToLive);
                if (timeout == 0) {
                    timeout = client.getReadTimeout();
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("GETnTOUCH : key [" + key + "], Status [" + futures.length + "]");
            }
            T data = null;
            for (Future<CASValue<T>> dataFuture : futures) {
                final T t = dataFuture.get(timeout, TimeUnit.MILLISECONDS).getValue();
                if (data == null) {
                    data = t;
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("GETnTOUCH : key [" + key + "], Value [" + data + "]");
            }
            if (data != null) {
                HIT_COUNTER.increment();
            } else {
                MISS_COUNTER.increment();
            }
            return data;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception executing getAndTouch key : " + key, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception executing getAndTouch key : " + key, ex);
        }
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<T> getAsynchronous(String key) throws EVCacheException {
        return this.getAsynchronous(key, (EVCacheTranscoder<T>) _defaultTranscoder);
    }


    /**
     * {@inheritDoc}
     */
    public <T> Future<T> getAsynchronous(String key, EVCacheTranscoder<T> tc) throws EVCacheException {
        if (null == key) {
            throw new IllegalArgumentException();
        }

        final EVCacheClient client = _pool.getEVCacheClient();
        if (client == null) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Could not find a client to asynchronously get the data");
            }
            return null;  // Fast failure
        }

        final Future<T> r;
        try {
            final String canonicalKey = getCanonicalizedKey(key);
            r = client.asyncGet(canonicalKey, tc);
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception while getting data for keys Asynchronously key : " + key, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception getting data for key : " + key, ex);
        }
        return r;
    }


    /**
     * {@inheritDoc}
     */
    public <T> Map<String, T> getBulk(Collection<String> keys, EVCacheTranscoder<T> tc) throws EVCacheException {
        if (null == keys) {
            throw new IllegalArgumentException();
        }
        if (keys.isEmpty()) {
            return Collections.<String, T>emptyMap();
        }

        final EVCacheClient client = _pool.getEVCacheClient();
        if (client == null) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Could not find a client to get the data in bulk");
            }
            return null;  // Fast failure
        }

        final Collection<String> canonicalKeys = new ArrayList<String>();

        /* Canonicalize keys and perform fast failure checking */
        for (String k : keys) {
            final String canonicalK = getCanonicalizedKey(k);
            canonicalKeys.add(canonicalK);
        }

        try {
            final Map<String, T> retMap = client.getBulk(canonicalKeys, tc);
            if (retMap == null || retMap.isEmpty()) {
                return Collections.<String, T>emptyMap();
            }

            /* Decanonicalize the keys */
            final Map<String, T> decanonicalR = new HashMap<String, T>(retMap.size() * 2);
            for (Map.Entry<String, T> i : retMap.entrySet()) {
                final String deCanKey = getKey(i.getKey());
                decanonicalR.put(deCanKey, i.getValue());
            }
            if (decanonicalR != null && !decanonicalR.isEmpty()) {
                BULK_HIT_COUNTER.increment();
            } else {
                BULK_MISS_COUNTER.increment();
            }
            if (log.isDebugEnabled()) {
                log.debug("BULK : Data [" + decanonicalR + "]");
            }
            return decanonicalR;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception getting bulk data for keys : " + keys, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception getting bulk data for keys : " + keys, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public <T> Map<String, T> getBulk(Collection<String> keys) throws EVCacheException {
        return (this.getBulk(keys, (EVCacheTranscoder<T>) _defaultTranscoder));
    }

    /**
     * {@inheritDoc}
     */
    public <T> Map<String, T> getBulk(String... keys) throws EVCacheException {
        return (this.getBulk(Arrays.asList(keys), (EVCacheTranscoder<T>) _defaultTranscoder));
    }

    /**
     * {@inheritDoc}
     */
    public <T> Map<String, T> getBulk(EVCacheTranscoder<T> tc, String... keys) throws EVCacheException {
        return (this.getBulk(Arrays.asList(keys), tc));
    }

    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc, int timeToLive) throws EVCacheException {
        if ((null == key) || (null == value)) {
            throw new IllegalArgumentException();
        }

        final EVCacheClient[] clients = _pool.getAllEVCacheClients();
        if (clients == null || clients.length == 0) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Could not find a client to set the data");
            }
            return new Future[0];  // Fast failure
        }

        try {
            final Future<Boolean>[] futures = new Future[clients.length];
            final String canonicalKey = getCanonicalizedKey(key);
            int index = 0;
            for (EVCacheClient client : clients) {
                futures[index++] = client.set(canonicalKey, tc, value, timeToLive);
            }
            if (log.isDebugEnabled()) {
                log.debug("SET : key [" + key + "], Status [" + futures.length + "]");
            }
            return futures;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception setting the data for key : " + key, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception setting data for key : " + key, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean>[] set(String key, T value, EVCacheTranscoder<T> tc) throws EVCacheException {
        return this.set(key, value, tc, _timeToLive);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean>[] set(String key, T value, int timeToLive)  throws EVCacheException {
        return this.set(key, value, (EVCacheTranscoder<T>) _defaultTranscoder, timeToLive);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Future<Boolean>[] set(String key, T value)  throws EVCacheException {
        return this.set(key, value, (EVCacheTranscoder<T>) _defaultTranscoder, _timeToLive);
    }

    /**
     * {@inheritDoc}
     */
    public Future<Boolean>[] delete(String key) throws EVCacheException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        final EVCacheClient[] clients = _pool.getAllEVCacheClients();
        if (clients == null || clients.length == 0) {
            NULL_CLIENT_COUNTER.increment();
            if (_throwException.get()) {
                throw new EVCacheException("Could not find a client to delete the key");
            }
            return new Future[0];  // Fast failure
        }

        try {
            final Future<Boolean>[] futures = new Future[clients.length];
            final String canonicalKey = getCanonicalizedKey(key);
            for (int i = 0; i < clients.length; i++) {
                futures[i] = clients[i].delete(canonicalKey);
            }
            return futures;
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Exception while deleting the data for key : " + key, ex);
            }
            if (!_throwException.get()) {
                return null;
            }
            throw new EVCacheException("Exception while deleting the data for key : " + key, ex);
        }
    }

    /**
     * The default TTL that will be used if one is not passed.
     *
     * @return the default TTL
     */
    public int getDefaultTTL() {
        return _timeToLive;
    }


    /**
     * The EVCache app that is used by this instance.
     *
     * @return the name of the evcache app
     */
    public String getAppName() {
        return _appName;
    }

    /**
     * The optional cache name that is used by this instance.
     *
     * @return the cache name or null if one is not provided
     */
    public String getCacheName() {
        return _cacheName;
    }

    /**
     * The String representation of this instance.
     */
    public String toString() {
        return "EVCacheImpl [App Name=" + _appName + ", Cache Name="
                + _cacheName + ", Default Transcoder=" + _defaultTranscoder
                + ", Zone Fallback=" + isInZoneFallback() + ", Default TTL="
                + getDefaultTTL() + ", EVCache Pool=" + _pool + ", throw Exception="
                + _throwException.get()
                + "]";
    }
}
