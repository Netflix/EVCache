package com.netflix.evcache;

import static com.netflix.evcache.util.Sneaky.sneakyThrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.ChainedDynamicProperty;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.event.EVCacheEvent;
import com.netflix.evcache.event.EVCacheEventListener;
import com.netflix.evcache.metrics.EVCacheMetricsFactory;
import com.netflix.evcache.metrics.Operation;
import com.netflix.evcache.metrics.Stats;
import com.netflix.evcache.operation.EVCacheFuture;
import com.netflix.evcache.operation.EVCacheLatchImpl;
import com.netflix.evcache.operation.EVCacheOperationFuture;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPool;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.EVCacheClientUtil;
import com.netflix.evcache.util.EVCacheConfig;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.Counter;
import com.netflix.spectator.api.DistributionSummary;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;
import rx.Observable;
import rx.Scheduler;
import rx.Single;

/**
 * An implementation of a ephemeral volatile cache.
 *
 * @author smadappa
 * @version 2.0
 */

@SuppressWarnings("unchecked")
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings({ "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "WMI_WRONG_MAP_ITERATOR",
    "DB_DUPLICATE_BRANCHES", "REC_CATCH_EXCEPTION",
"RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE" })
final public class EVCacheImpl implements EVCache {

    private static Logger log = LoggerFactory.getLogger(EVCacheImpl.class);

    private final String _appName;
    private final String _cacheName;
    private final String _metricPrefix;
    private final String _metricName;
    private final Transcoder<?> _transcoder;
    private final boolean _zoneFallback;
    private final boolean _throwException;

    private final int _timeToLive; // defaults to 15 minutes
    private final EVCacheClientPool _pool;

    private final ChainedDynamicProperty.BooleanProperty _throwExceptionFP, _zoneFallbackFP;
    private final DynamicBooleanProperty _bulkZoneFallbackFP;
    private final DynamicBooleanProperty _bulkPartialZoneFallbackFP;
    private final ChainedDynamicProperty.BooleanProperty _useInMemoryCache;
    private final Stats stats;
    private EVCacheInMemoryCache<?> cache;
    private EVCacheClientUtil clientUtil = null;

    private final EVCacheClientPoolManager _poolManager;
    private DistributionSummary setTTLSummary, replaceTTLSummary, touchTTLSummary, setDataSizeSummary, replaceDataSizeSummary, appendDataSizeSummary;
    private Counter touchCounter;

    EVCacheImpl(String appName, String cacheName, int timeToLive, Transcoder<?> transcoder, boolean enableZoneFallback,
            boolean throwException,
            EVCacheClientPoolManager poolManager) {
        this._appName = appName;
        this._cacheName = cacheName;
        this._timeToLive = timeToLive;
        this._transcoder = transcoder;
        this._zoneFallback = enableZoneFallback;
        this._throwException = throwException;

        stats = EVCacheMetricsFactory.getStats(appName, cacheName);
        _metricName = (_cacheName == null) ? _appName : _appName + "." + _cacheName;
        _metricPrefix = _appName + "-";
        this._poolManager = poolManager;
        this._pool = poolManager.getEVCacheClientPool(_appName);
        final EVCacheConfig config = EVCacheConfig.getInstance();
        _throwExceptionFP = config.getChainedBooleanProperty(_metricName + ".throw.exception", _appName + ".throw.exception", Boolean.FALSE);
        _zoneFallbackFP = config.getChainedBooleanProperty(_metricName + ".fallback.zone", _appName + ".fallback.zone", Boolean.TRUE);
        _bulkZoneFallbackFP = config.getDynamicBooleanProperty(_appName + ".bulk.fallback.zone", true);
        _bulkPartialZoneFallbackFP = config.getDynamicBooleanProperty(_appName+ ".bulk.partial.fallback.zone", true);
        _useInMemoryCache = config.getChainedBooleanProperty(_appName + ".use.inmemory.cache", "evcache.use.inmemory.cache", Boolean.FALSE);
        _pool.pingServers();
    }

    private String getCanonicalizedKey(String key) {
        final String cKey;
        if (this._cacheName == null) {
            cKey = key;
        } else {
            cKey = _cacheName + ':' + key;
        }
        StringUtils.validateKey(cKey, false);
        return  cKey;
    }

    private String getKey(String canonicalizedKey) {
        if (canonicalizedKey == null) return canonicalizedKey;
        if (_cacheName == null) return canonicalizedKey;
        final String _cacheNameDelimited = _cacheName + ':';
        return canonicalizedKey.replaceFirst(_cacheNameDelimited, "");
    }

    private boolean hasZoneFallbackForBulk() {
        if (!_pool.supportsFallback()) return false;
        if (!_bulkZoneFallbackFP.get()) return false;
        return _zoneFallback;
    }

    private boolean hasZoneFallback() {
        if (!_pool.supportsFallback()) return false;
        if (!_zoneFallbackFP.get().booleanValue()) return false;
        return _zoneFallback;
    }

    private boolean shouldLog() {
        return _poolManager.shouldLog(_appName);
    }

    private boolean doThrowException() {
        return (_throwException || _throwExceptionFP.get().booleanValue());
    }

    private List<EVCacheEventListener> getEVCacheEventListeners() {
        return _poolManager.getEVCacheEventListeners();
    }

    private EVCacheEvent createEVCacheEvent(Collection<EVCacheClient> clients, Collection<String> keys, Call call) {
        final List<EVCacheEventListener> evcacheEventListenerList = getEVCacheEventListeners();
        if (evcacheEventListenerList == null || evcacheEventListenerList.size() == 0) return null;

        final EVCacheEvent event = new EVCacheEvent(call, _appName, _cacheName);
        event.setKeys(keys);
        event.setClients(clients);
        return event;
    }

    private boolean shouldThrottle(EVCacheEvent event) throws EVCacheException {
        for (EVCacheEventListener evcacheEventListener : getEVCacheEventListeners()) {
            if (evcacheEventListener.onThrottle(event)) {
                return true;
            }
        }
        return false;
    }

    private void startEvent(EVCacheEvent event) {
        final List<EVCacheEventListener> evcacheEventListenerList = getEVCacheEventListeners();
        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
            evcacheEventListener.onStart(event);
        }
    }

    private void endEvent(EVCacheEvent event) {
        final List<EVCacheEventListener> evcacheEventListenerList = getEVCacheEventListeners();
        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
            evcacheEventListener.onComplete(event);
        }
    }

    private void eventError(EVCacheEvent event, Throwable t) {
        final List<EVCacheEventListener> evcacheEventListenerList = getEVCacheEventListeners();
        for (EVCacheEventListener evcacheEventListener : evcacheEventListenerList) {
            evcacheEventListener.onError(event, t);
        }
    }

    private <T> EVCacheInMemoryCache<T> getInMemoryCache(Transcoder<T> tc) {
        if (cache == null) cache = _poolManager.createInMemoryCache(_appName, tc, this);
        return (EVCacheInMemoryCache<T>) cache;
    }

    public <T> T get(String key) throws EVCacheException {
        return this.get(key, (Transcoder<T>) _transcoder);
    }

    private void increment(String metric) {
        increment(null, null, _metricPrefix + metric);
    }

    private void increment(String serverGroup, String cachePrefix, String metric) {
        EVCacheMetricsFactory.increment(_appName, cachePrefix, serverGroup, _metricPrefix + metric);
    }

    public <T> T get(String key, Transcoder<T> tc) throws EVCacheException {
        if (null == key) throw new IllegalArgumentException("Key cannot be null");
    	final String canonicalKey = getCanonicalizedKey(key);
        if (_useInMemoryCache.get()) {
            T value = null;
			try {
				value = (T) getInMemoryCache(tc).get(canonicalKey);
			} catch (ExecutionException e) {
				if (log.isDebugEnabled() && shouldLog()) log.debug("ExecutionException while getting data from InMemory Cache", e);
				final boolean throwExc = doThrowException();
				if(throwExc) {
					if(e.getCause() instanceof EVCacheException) {
						throw (EVCacheException)e.getCause();
					} 
					throw new EVCacheException("ExecutionException", e);
				}
			}
            if (log.isDebugEnabled() && shouldLog()) log.debug("Value retrieved from inmemory cache for APP " + _appName + ", key : " + canonicalKey + (log.isTraceEnabled() ? "; value : " + value : ""));
            if (value != null) return value;
        }
        return doGet(canonicalKey, tc);
    }


    <T> T doGet(String canonicalKey , Transcoder<T> tc) throws EVCacheException {   
        final boolean throwExc = doThrowException();
        EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to get the data APP " + _appName);
            return null; // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), Collections.singletonList(canonicalKey), Call.GET);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + canonicalKey);
                    return null;
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.GET, stats, Operation.TYPE.MILLI);
        try {
            final boolean hasZF = hasZoneFallback();
            boolean throwEx = hasZF ? false : throwExc;
            T data = getData(client, canonicalKey, tc, throwEx, hasZF);
            if (data == null && hasZF) {
                final List<EVCacheClient> fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                if (fbClients != null && !fbClients.isEmpty()) {
                    for (int i = 0; i < fbClients.size(); i++) {
                        final EVCacheClient fbClient = fbClients.get(i);
                        if(i >= fbClients.size() - 1) throwEx = throwExc;
                        data = getData(fbClient, canonicalKey, tc, throwEx, (i < fbClients.size() - 1) ? true : false);
                        if (log.isDebugEnabled() && shouldLog()) log.debug("Retry for APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "") + "], ServerGroup : " + fbClient.getServerGroup());
                        if (data != null) {
                            client = fbClient;
                            break;
                        }
                    }
                    increment(client.getServerGroupName(), _cacheName, "RETRY_" + ((data == null) ? "MISS" : "HIT"));
                }
            }
            if (data != null) {
                stats.cacheHit(Call.GET);
                if (event != null) event.setAttribute("status", "GHIT");
            } else {
                stats.cacheMiss(Call.GET);
                if (event != null) event.setAttribute("status", "GMISS");
                if (log.isInfoEnabled() && shouldLog()) log.info("GET : APP " + _appName + " ; cache miss for key : " + canonicalKey);
            }
            if (log.isDebugEnabled() && shouldLog()) log.debug("GET : APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "") + "], ServerGroup : " + client.getServerGroup());
            if (event != null) endEvent(event);
            return data;
        } catch (net.spy.memcached.internal.CheckedOperationTimeoutException ex) {
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("CheckedOperationTimeoutException getting data for APP " + _appName + ", key = "
                    + canonicalKey
                    + ".\nYou can set the following property to increase the timeout " + _appName
                    + ".EVCacheClientPool.readTimeout=<timeout in milli-seconds>", ex);
        } catch (Exception ex) {
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("Exception getting data for APP " + _appName + ", key = " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("GET : APP " + _appName + ", Took " + op.getDuration() + " milliSec.");
        }
    }

    public <T> Single<T> get(String key, Scheduler scheduler) {
        return this.get(key, (Transcoder<T>) _transcoder, scheduler);
    }

    public <T> Single<T> get(String key, Transcoder<T> tc, Scheduler scheduler) {
        if (null == key) return Single.error(new IllegalArgumentException("Key cannot be null"));

        final boolean throwExc = doThrowException();
        final EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            return Single.error(new EVCacheException("Could not find a client to get the data APP " + _appName));
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), Collections.singletonList(key), Call.GET);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    return Single.error(new EVCacheException("Request Throttled for app " + _appName + " & key " + key));
                }
            } catch(EVCacheException ex) {
                throw sneakyThrow(ex);
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.GET, stats, Operation.TYPE.MILLI);
        final boolean hasZF = hasZoneFallback();
        boolean throwEx = hasZF ? false : throwExc;
        return getData(client, canonicalKey, tc, throwEx, hasZF, scheduler).flatMap(data -> {
            if (data == null && hasZF) {
                final List<EVCacheClient> fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                if (fbClients != null && !fbClients.isEmpty()) {
                    return Observable.concat(Observable.from(fbClients).map(
                            fbClient -> getData(fbClients.indexOf(fbClient), fbClients.size(), fbClient, canonicalKey, tc, throwEx, throwExc, false, scheduler) //TODO : for the last one make sure to pass throwExc
                            .doOnSuccess(fbData -> increment(fbClient.getServerGroupName(), _cacheName, "RETRY_" + ((fbData == null) ? "MISS" : "HIT")))
                            .toObservable()))
                            .firstOrDefault(null, fbData -> (fbData != null)).toSingle();
                }
            }
            return Single.just(data);
        }).map(data -> {
            if (data != null) {
                stats.cacheHit(Call.GET);
                if (event != null) event.setAttribute("status", "GHIT");
            } else {
                stats.cacheMiss(Call.GET);
                if (event != null) event.setAttribute("status", "GMISS");
                if (log.isInfoEnabled() && shouldLog())
                    log.info("GET : APP " + _appName + " ; cache miss for key : " + canonicalKey);
            }
            if (log.isDebugEnabled() && shouldLog()) log.debug("GET : APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "")  + "], ServerGroup : " + client .getServerGroup());
            if (event != null) endEvent(event);
            return data;
        }).onErrorReturn(ex -> {
            if (ex instanceof net.spy.memcached.internal.CheckedOperationTimeoutException) {
                if (event != null) eventError(event, ex);
                if (!throwExc) return null;
                throw sneakyThrow(new EVCacheException("CheckedOperationTimeoutException getting data for APP " + _appName + ", key = "
                        + canonicalKey
                        + ".\nYou can set the following property to increase the timeout " + _appName
                        + ".EVCacheClientPool.readTimeout=<timeout in milli-seconds>", ex));
            } else {
                if (event != null) eventError(event, ex);
                if (!throwExc) return null;
                throw sneakyThrow(new EVCacheException("Exception getting data for APP " + _appName + ", key = " + canonicalKey, ex));
            }
        }).doAfterTerminate(() -> {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("GET : APP " + _appName + ", Took " + op.getDuration() + " milliSec.");
        });
    }

    private <T> T getData(EVCacheClient client, String canonicalKey, Transcoder<T> tc, boolean throwException, boolean hasZF) throws Exception {
        if (client == null) return null;
        try {
            if(tc == null && _transcoder != null) tc = (Transcoder<T>)_transcoder;
            return client.get(canonicalKey, tc, throwException, hasZF);
        } catch (EVCacheReadQueueException ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("EVCacheReadQueueException while getting data for APP " + _appName + ", key : " + canonicalKey + "; hasZF : " + hasZF, ex);
            if (!throwException || hasZF) return null;
            throw ex;
        } catch (EVCacheException ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("EVCacheException while getting data for APP " + _appName + ", key : " + canonicalKey + "; hasZF : " + hasZF, ex);
            if (!throwException || hasZF) return null;
            throw ex;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception while getting data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (!throwException || hasZF) return null;
            throw ex;
        }
    }

    private <T> Single<T> getData(int index, int size, EVCacheClient client, String canonicalKey, Transcoder<T> tc, boolean throwEx, boolean throwExc, boolean hasZF, Scheduler scheduler) {
        if(index >= size -1) throwEx = throwExc; 
        return getData(client, canonicalKey, tc, throwEx, hasZF, scheduler);
    }

    private <T> Single<T> getData(EVCacheClient client, String canonicalKey, Transcoder<T> tc, boolean throwException, boolean hasZF, Scheduler scheduler) {
        if (client == null) return Single.error(new IllegalArgumentException("Client cannot be null"));
        if(tc == null && _transcoder != null) tc = (Transcoder<T>)_transcoder;
        return client.get(canonicalKey, tc, throwException, hasZF, scheduler).onErrorReturn(ex -> {
            if (ex instanceof EVCacheReadQueueException) {
                if (log.isDebugEnabled() && shouldLog()) log.debug("EVCacheReadQueueException while getting data for APP " + _appName + ", key : " + canonicalKey + "; hasZF : " + hasZF, ex);
                if (!throwException || hasZF) return null;
                throw sneakyThrow(ex);
            } else if (ex instanceof EVCacheException) {
                if (log.isDebugEnabled() && shouldLog()) log.debug("EVCacheException while getting data for APP " + _appName + ", key : " + canonicalKey + "; hasZF : " + hasZF, ex);
                if (!throwException || hasZF) return null;
                throw sneakyThrow(ex);
            } else {
                if (log.isDebugEnabled() && shouldLog()) log.debug("Exception while getting data for APP " + _appName + ", key : " + canonicalKey, ex);
                if (!throwException || hasZF) return null;
                throw sneakyThrow(ex);
            }
        });
    }

    public <T> T getAndTouch(String key, int timeToLive) throws EVCacheException {
        return this.getAndTouch(key, timeToLive, (Transcoder<T>) _transcoder);
    }


    public <T> Single<T> getAndTouch(String key, int timeToLive, Scheduler scheduler) {
        return this.getAndTouch(key, timeToLive, (Transcoder<T>) _transcoder, scheduler);
    }

    public <T> Single<T> getAndTouch(String key, int timeToLive, Transcoder<T> tc, Scheduler scheduler) {
        if (null == key) return Single.error(new IllegalArgumentException("Key cannot be null"));

        final boolean throwExc = doThrowException();
        final EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            return Single.error(new EVCacheException("Could not find a client to get and touch the data for APP " + _appName));
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), Collections.singletonList(key), Call.GET_AND_TOUCH);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    return Single.error(new EVCacheException("Request Throttled for app " + _appName + " & key " + key));
                }
            } catch(EVCacheException ex) {
                throw sneakyThrow(ex);
            }
            event.setTTL(timeToLive);
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.GET_AND_TOUCH, stats, Operation.TYPE.MILLI);
        final boolean hasZF = hasZoneFallback();
        boolean throwEx = hasZF ? false : throwExc;
        //anyway we have to touch all copies so let's just reuse getData instead of getAndTouch
        return getData(client, canonicalKey, tc, throwEx, hasZF, scheduler).flatMap(data -> {
            if (data == null && hasZF) {
                final List<EVCacheClient> fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                if (fbClients != null && !fbClients.isEmpty()) {
                    return Observable.concat(Observable.from(fbClients).map(
                            fbClient -> getData(fbClients.indexOf(fbClient), fbClients.size(), fbClient, canonicalKey, tc, throwEx, throwExc, false, scheduler) //TODO : for the last one make sure to pass throwExc
                            .doOnSuccess(fbData -> increment(fbClient.getServerGroupName(), _cacheName, "RETRY_" + ((fbData == null) ? "MISS" : "HIT")))
                            .toObservable()))
                            .firstOrDefault(null, fbData -> (fbData != null)).toSingle();
                }
            }
            return Single.just(data);
        }).map(data -> {
            if (data != null) {
                stats.cacheHit(Call.GET_AND_TOUCH);
                if (event != null) event.setAttribute("status", "THIT");
                // touch all copies
                try {
                	touchData(canonicalKey, key, timeToLive);
                } catch (Exception e) {
                    throw sneakyThrow(new EVCacheException("Exception performing touch for APP " + _appName + ", key = " + canonicalKey, e));
                }
                if (log.isDebugEnabled() && shouldLog()) log.debug("GET_AND_TOUCH : APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "") + "], ServerGroup : " + client .getServerGroup());
            } else {
                stats.cacheMiss(Call.GET_AND_TOUCH);
                if (event != null) event.setAttribute("status", "TMISS");
                if (log.isInfoEnabled() && shouldLog()) log.info("GET_AND_TOUCH : APP " + _appName + " ; cache miss for key : " + canonicalKey);
            }
            if (event != null) endEvent(event);
            return data;
        }).onErrorReturn(ex -> {
            if (ex instanceof net.spy.memcached.internal.CheckedOperationTimeoutException) {
                if (event != null) eventError(event, ex);
                if (!throwExc) return null;
                throw sneakyThrow(new EVCacheException("CheckedOperationTimeoutException executing getAndTouch APP " + _appName + ", key = " + canonicalKey
                        + ".\nYou can set the following property to increase the timeout " + _appName + ".EVCacheClientPool.readTimeout=<timeout in milli-seconds>", ex));
            } else {
                if (event != null) eventError(event, ex);
                if (!throwExc) return null;
                throw sneakyThrow(new EVCacheException("Exception executing getAndTouch APP " + _appName + ", key = " + canonicalKey, ex));
            }
        }).doAfterTerminate(() -> {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("GET_AND_TOUCH : APP " + _appName + ", Took " + op.getDuration() + " milliSec.");
        });
    }

    @Override
    public <T> T getAndTouch(String key, int timeToLive, Transcoder<T> tc) throws EVCacheException {
        if (null == key) throw new IllegalArgumentException("Key cannot be null");
        final String canonicalKey = getCanonicalizedKey(key);
        if (_useInMemoryCache.get()) {
            final boolean throwExc = doThrowException();
            T value = null;
			try {
				value = (T) getInMemoryCache(tc).get(canonicalKey);
			} catch (ExecutionException e) {
				if (log.isDebugEnabled() && shouldLog()) log.debug("ExecutionException while getting data from InMemory Cache", e);
				if(throwExc) {
					if(e.getCause() instanceof EVCacheException) {
						throw (EVCacheException)e.getCause();
					} 
					throw new EVCacheException("ExecutionException", e);
				}
			}
            if (value != null) {
            	try {
					touchData(canonicalKey, key, timeToLive);
				} catch (Exception e) {
		            if (throwExc) throw new EVCacheException("Exception executing getAndTouch APP " + _appName + ", key = " + canonicalKey, e);
				}
                return value;
            }
        }
        return doGetAndTouch(canonicalKey, key, timeToLive, tc);
    }
    
    <T> T doGetAndTouch(String canonicalKey, String key, int timeToLive, Transcoder<T> tc) throws EVCacheException {
        final boolean throwExc = doThrowException();
        EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to get and touch the data for App " + _appName);
            return null; // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), Collections.singletonList(canonicalKey), Call.GET_AND_TOUCH);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + canonicalKey);
                    return null;
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            event.setTTL(timeToLive);
            startEvent(event);
        }

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.GET_AND_TOUCH, stats, Operation.TYPE.MILLI);
        try {
            final boolean hasZF = hasZoneFallback();
            boolean throwEx = hasZF ? false : throwExc;
            //T data = getAndTouchData(client, canonicalKey, tc, throwEx, hasZF, timeToLive);
            T data = getData(client, canonicalKey, tc, throwEx, hasZF);
            if (data == null && hasZF) {
                final List<EVCacheClient> fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                for (int i = 0; i < fbClients.size(); i++) {
                    final EVCacheClient fbClient = fbClients.get(i);
                    if(i >= fbClients.size() - 1) throwEx = throwExc;
                    data = getData(fbClient, canonicalKey, tc, throwEx, (i < fbClients.size() - 1) ? true : false);
                    if (log.isDebugEnabled() && shouldLog()) log.debug("GetAndTouch Retry for APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "")  + "], ServerGroup : " + fbClient.getServerGroup());
                    if (data != null) {
                        client = fbClient;
                        break;
                    }
                }
                increment(client.getServerGroupName(), _cacheName, "RETRY_" + ((data == null) ? "MISS" : "HIT"));
            }

            if (data != null) {
                stats.cacheHit(Call.GET_AND_TOUCH);
                if (event != null) event.setAttribute("status", "THIT");

                // touch all copies
                touchData(canonicalKey, key, timeToLive);
                if (log.isDebugEnabled() && shouldLog()) log.debug("GET_AND_TOUCH : APP " + _appName + ", key [" + canonicalKey + (log.isTraceEnabled() ? "], Value [" + data : "") + "], ServerGroup : " + client.getServerGroup());
            } else {
                stats.cacheMiss(Call.GET_AND_TOUCH);
                if (log.isInfoEnabled() && shouldLog()) log.info("GET_AND_TOUCH : APP " + _appName + " ; cache miss for key : " + canonicalKey);
                if (event != null) event.setAttribute("status", "TMISS");
            }
            if (event != null) endEvent(event);
            return data;
        } catch (net.spy.memcached.internal.CheckedOperationTimeoutException ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("CheckedOperationTimeoutException executing getAndTouch APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("CheckedOperationTimeoutException executing getAndTouch APP " + _appName + ", key  = " + canonicalKey
                    + ".\nYou can set the following property to increase the timeout " + _appName+ ".EVCacheClientPool.readTimeout=<timeout in milli-seconds>", ex);
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception executing getAndTouch APP " + _appName + ", key = " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("Exception executing getAndTouch APP " + _appName + ", key = " + canonicalKey,
                    ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("Took " + op.getDuration() + " milliSec to get&Touch the value for APP " + _appName + ", key " + canonicalKey);
        }
    }

	@Override
    public Future<Boolean>[] touch(String key, int timeToLive) throws EVCacheException {
        final EVCacheLatch latch = this.touch(key, timeToLive, null);
        if (latch == null) return new EVCacheFuture[0];
        final List<Future<Boolean>> futures = latch.getAllFutures();
        if (futures == null || futures.isEmpty()) return new EVCacheFuture[0];
        final EVCacheFuture[] eFutures = new EVCacheFuture[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            final Future<Boolean> future = futures.get(i);
            if (future instanceof EVCacheFuture) {
                eFutures[i] = (EVCacheFuture) future;
            } else if (future instanceof EVCacheOperationFuture) {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, ((EVCacheOperationFuture<Boolean>) futures.get(i)).getServerGroup());
            } else {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, null);
            }
        }
        return eFutures;
	}


	public <T> EVCacheLatch touch(String key, int timeToLive, Policy policy) throws EVCacheException {
        if (null == key) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to set the data");
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.TOUCH);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        try {
            final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy == null ? Policy.ALL_MINUS_1 : policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);
            touchData(canonicalKey, key, timeToLive, clients, latch);

            if (touchTTLSummary == null) this.touchTTLSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-TouchData-TTL", _appName, null);
            if (touchTTLSummary != null) touchTTLSummary.record(timeToLive);

            if (touchCounter == null) this.touchCounter = EVCacheMetricsFactory.getCounter(_appName, _cacheName, _metricPrefix + "TouchCall", DataSourceType.COUNTER);
            if (touchCounter != null) touchCounter.increment();
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setTTL(timeToLive);
                event.setLatch(latch);
                endEvent(event);
            }
            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception touching the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName);
            throw new EVCacheException("Exception setting data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            if (log.isDebugEnabled() && shouldLog()) log.debug("TOUCH : APP " + _appName + " for key : " + canonicalKey + " with ttl : " + timeToLive);
        }
    }
    
    private EVCacheFuture[] touchData(String canonicalKey, String key, int timeToLive) throws Exception {
    	final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
    	return touchData(canonicalKey, key, timeToLive, clients);
    }
    
    private EVCacheFuture[] touchData(String canonicalKey, String key, int timeToLive, EVCacheClient[] clients) throws Exception {
    	return touchData(canonicalKey, key, timeToLive, clients, null);
    }

    private EVCacheFuture[] touchData(String canonicalKey, String key, int timeToLive, EVCacheClient[] clients, EVCacheLatch latch ) throws Exception {
        final EVCacheFuture[] futures = new EVCacheFuture[clients.length];
        int index = 0;
        for (EVCacheClient client : clients) {
            final Future<Boolean> future = client.touch(canonicalKey, timeToLive, latch);
            futures[index++] = new EVCacheFuture(future, key, _appName, client.getServerGroup());
        }
    	return futures;
    }

    public <T> Future<T> getAsynchronous(String key) throws EVCacheException {
        return this.getAsynchronous(key, (Transcoder<T>) _transcoder);
    };

    @Override
    public <T> Future<T> getAsynchronous(String key, Transcoder<T> tc) throws EVCacheException {
        if (null == key) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to asynchronously get the data");
            return null; // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), Collections.singletonList(key),
                Call.ASYNC_GET);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return null;
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final Future<T> r;
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.ASYNC_GET, stats,
                Operation.TYPE.MILLI);
        try {
            final String canonicalKey = getCanonicalizedKey(key);
            if(tc == null && _transcoder != null) tc = (Transcoder<T>)_transcoder;
            r = client.asyncGet(canonicalKey, tc, throwExc, false);
            if (event != null) endEvent(event);
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug( "Exception while getting data for keys Asynchronously APP " + _appName + ", key : " + key, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("Exception getting data for APP " + _appName + ", key : " + key, ex);
        } finally {
            op.stop();
        }

        return r;
    }

    private <T> Map<String, T> getBulkData(EVCacheClient client, Collection<String> canonicalKeys, Transcoder<T> tc,
            boolean throwException, boolean hasZF) throws Exception {
        try {
            if(tc == null && _transcoder != null) tc = (Transcoder<T>)_transcoder;
            return client.getBulk(canonicalKeys, tc, throwException, hasZF);
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception while getBulk data for APP " + _appName + ", key : " + canonicalKeys, ex);
            if (!throwException || hasZF) return null;
            throw ex;
        }
    }

    public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc) throws EVCacheException {
        return getBulk(keys, tc, false, 0);
    }

    public <T> Map<String, T> getBulkAndTouch(Collection<String> keys, Transcoder<T> tc, int timeToLive)
            throws EVCacheException {
        return getBulk(keys, tc, true, timeToLive);
    }

    private <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc, boolean touch, int ttl)
            throws EVCacheException {
        if (null == keys) throw new IllegalArgumentException();
        if (keys.isEmpty()) return Collections.<String, T> emptyMap();

        final boolean throwExc = doThrowException();
        EVCacheClient client = _pool.getEVCacheClientForRead();
        if (client == null) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to get the data in bulk");
            return Collections.<String, T> emptyMap();// Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Collections.singletonList(client), keys, Call.BULK);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & keys " + keys);
                    return Collections.<String, T> emptyMap();
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            event.setTTL(ttl);
            startEvent(event);
        }

        final Collection<String> canonicalKeys = new ArrayList<String>();
        /* Canonicalize keys and perform fast failure checking */
        for (String k : keys) {
            final String canonicalK = getCanonicalizedKey(k);
            canonicalKeys.add(canonicalK);
        }

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.BULK, stats, Operation.TYPE.MILLI);
        try {
            final boolean hasZF = hasZoneFallbackForBulk();
            boolean throwEx = hasZF ? false : throwExc;
            increment(client.getServerGroupName(), _cacheName, "BULK_GET");
            Map<String, T> retMap = getBulkData(client, canonicalKeys, tc, throwEx, hasZF);
            List<EVCacheClient> fbClients = null;
            if (hasZF) {
                if (retMap == null || retMap.isEmpty()) {
                    fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                    if (fbClients != null && !fbClients.isEmpty()) {
                        for (int i = 0; i < fbClients.size(); i++) {
                            final EVCacheClient fbClient = fbClients.get(i);
                            if(i >= fbClients.size() - 1) throwEx = throwExc;
                            retMap = getBulkData(fbClient, canonicalKeys, tc, throwEx, (i < fbClients.size() - 1) ? true : false);
                            if (log.isDebugEnabled() && shouldLog()) log.debug("Fallback for APP " + _appName + ", key [" + canonicalKeys + (log.isTraceEnabled() ? "], Value [" + retMap : "") + "], zone : " + fbClient.getZone());
                            if (retMap != null && !retMap.isEmpty()) break;
                        }
                        increment(client.getServerGroupName(), _cacheName, "BULK_GET-FULL_RETRY-" + ((retMap == null || retMap.isEmpty()) ? "MISS" : "HIT"));
                    }
                }

                if (retMap != null && keys.size() > retMap.size() && _bulkPartialZoneFallbackFP.get()) {
                    final int initRetMapSize = retMap.size();
                    final int initRetrySize = keys.size() - retMap.size();
                    List<String> retryKeys = new ArrayList<String>(initRetrySize);
                    for (Iterator<String> keysItr = canonicalKeys.iterator(); keysItr.hasNext();) {
                        final String key = keysItr.next();
                        if (!retMap.containsKey(key)) {
                            retryKeys.add(key);
                        }
                    }

                    fbClients = _pool.getEVCacheClientsForReadExcluding(client.getServerGroup());
                    if (fbClients != null && !fbClients.isEmpty()) {
                        for (int ind = 0; ind < fbClients.size(); ind++) {
                            final EVCacheClient fbClient = fbClients.get(ind);
                            final Map<String, T> fbRetMap = getBulkData(fbClient, retryKeys, tc, false, hasZF);
                            if (log.isDebugEnabled() && shouldLog()) log.debug("Fallback for APP " + _appName + ", key [" + retryKeys + "], Fallback Server Group : " + fbClient .getServerGroup().getName());
                            for (Map.Entry<String, T> i : fbRetMap.entrySet()) {
                                retMap.put(i.getKey(), i.getValue());
                                if (log.isDebugEnabled() && shouldLog()) log.debug("Fallback for APP " + _appName + ", key [" + i.getKey() + (log.isTraceEnabled() ? "], Value [" + i.getValue(): "]"));
                            }
                            if (retryKeys.size() == fbRetMap.size()) break;
                            if (ind < fbClients.size()) {
                                retryKeys = new ArrayList<String>(keys.size() - retMap.size());
                                for (Iterator<String> keysItr = canonicalKeys.iterator(); keysItr.hasNext();) {
                                    final String key = keysItr.next();
                                    if (!retMap.containsKey(key)) {
                                        retryKeys.add(key);
                                    }
                                }
                            }
                        }
                        if (retMap.size() > initRetMapSize) increment(client.getServerGroupName(), _cacheName, "BULK_GET-PARTIAL_RETRY-" + (retMap.isEmpty() ? "MISS" : "HIT"));
                    }
                    if (log.isDebugEnabled() && shouldLog() && retMap.size() == keys.size()) log.debug("Fallback SUCCESS for APP " + _appName + ",  retMap [" + retMap + "]");
                }
            }

            if (retMap == null || retMap.isEmpty()) {
                if (log.isInfoEnabled() && shouldLog()) log.info("BULK : APP " + _appName + " ; Full cache miss for keys : " + keys);
                if (event != null) event.setAttribute("status", "BMISS_ALL");

                if (retMap != null && retMap.isEmpty()) {
                    retMap = new HashMap<String, T>();
                    for (String k : keys) {
                        retMap.put(k, null);
                    }
                }
                stats.cacheMiss(Call.BULK);
                /* If both Retry and first request fail Exit Immediately. */
                increment(client.getServerGroupName(), _cacheName, "BULK_MISS");
                if (event != null) endEvent(event);
                return retMap;
            }

            /* Decanonicalize the keys */
            final Map<String, T> decanonicalR = new HashMap<String, T>((canonicalKeys.size() * 4) / 3 + 1);
            for (Iterator<String> itr = canonicalKeys.iterator(); itr.hasNext();) {
                final String key = itr.next();
                final String deCanKey = getKey(key);
                final T value = retMap.get(key);
                if (value != null) {
                    decanonicalR.put(deCanKey, value);
                    if (touch) touchData(key, deCanKey, ttl);
                } else if (fbClients != null && fbClients.size() > 0) {
                    // this ensures the fallback was tried
                    decanonicalR.put(deCanKey, null);
                }
            }
            if (!decanonicalR.isEmpty()) {
                if (decanonicalR.size() == keys.size()) {
                    stats.cacheHit(Call.BULK);
                    increment(client.getServerGroupName(), _cacheName, "BULK_HIT");
                    if (event != null) event.setAttribute("status", "BHIT");
                } else {
                    if (event != null) {
                        event.setAttribute("status", "BHIT_PARTIAL");
                        event.setAttribute("BHIT_PARTIAL_KEYS", decanonicalR);
                    }
                    increment(client.getServerGroupName(), _cacheName, "BULK_HIT_PARTIAL");
                    if (log.isInfoEnabled() && shouldLog()) log.info("BULK_HIT_PARTIAL for APP " + _appName + ", keys in cache [" + decanonicalR + "], all keys [" + keys + "]");
                }
            }

            if (log.isDebugEnabled() && shouldLog()) log.debug("APP " + _appName + ", BULK : Data [" + decanonicalR + "]");
            if (event != null) endEvent(event);
            return decanonicalR;
        } catch (net.spy.memcached.internal.CheckedOperationTimeoutException ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("CheckedOperationTimeoutException getting bulk data for APP " + _appName + ", keys : " + canonicalKeys, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("CheckedOperationTimeoutException getting bulk data for APP " + _appName + ", keys = " + canonicalKeys
                    + ".\nYou can set the following property to increase the timeout " + _appName + ".EVCacheClientPool.bulkReadTimeout=<timeout in milli-seconds>", ex);
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception getting bulk data for APP " + _appName + ", keys = " + canonicalKeys, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return null;
            throw new EVCacheException("Exception getting bulk data for APP " + _appName + ", keys = " + canonicalKeys, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("BULK : APP " + _appName + " Took " + op.getDuration() + " milliSec to get the value for key " + canonicalKeys);
        }
    }

    public <T> Map<String, T> getBulk(Collection<String> keys) throws EVCacheException {
        return (this.getBulk(keys, (Transcoder<T>) _transcoder));
    }

    public <T> Map<String, T> getBulk(String... keys) throws EVCacheException {
        return (this.getBulk(Arrays.asList(keys), (Transcoder<T>) _transcoder));
    }

    public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) throws EVCacheException {
        return (this.getBulk(Arrays.asList(keys), tc));
    }

    @Override
    public <T> EVCacheFuture[] set(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException {
        final EVCacheLatch latch = this.set(key, value, tc, timeToLive, null);
        if (latch == null) return new EVCacheFuture[0];
        final List<Future<Boolean>> futures = latch.getAllFutures();
        if (futures == null || futures.isEmpty()) return new EVCacheFuture[0];
        final EVCacheFuture[] eFutures = new EVCacheFuture[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            final Future<Boolean> future = futures.get(i);
            if (future instanceof EVCacheFuture) {
                eFutures[i] = (EVCacheFuture) future;
            } else if (future instanceof EVCacheOperationFuture) {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, ((EVCacheOperationFuture<T>) futures.get(
                        i)).getServerGroup());
            } else {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, null);
            }
        }
        return eFutures;
    }

    public <T> EVCacheLatch set(String key, T value, Policy policy) throws EVCacheException {
        return set(key, value, (Transcoder<T>)_transcoder, _timeToLive, policy);
    }

    public <T> EVCacheLatch set(String key, T value, int timeToLive, Policy policy) throws EVCacheException {
        return set(key, value, (Transcoder<T>)_transcoder, timeToLive, policy);
    }

    public <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, EVCacheLatch.Policy policy) throws EVCacheException {
        return set(key, value, tc, _timeToLive, policy);
    }

    public <T> EVCacheLatch set(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy) throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to set the data");
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.SET);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName);
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.SET, stats, Operation.TYPE.MILLI);
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy == null ? Policy.ALL_MINUS_1 : policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);
        try {
            CachedData cd = null;
            for (EVCacheClient client : clients) {
                if (cd == null) {
                    if (tc != null) {
                        cd = tc.encode(value);
                    } else if ( _transcoder != null) { 
                        cd = ((Transcoder<Object>)_transcoder).encode(value);
                    } else {
                        cd = client.getTranscoder().encode(value);
                    }

                    if (setTTLSummary == null) this.setTTLSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-SetData-TTL", _appName, null);
                    if (setTTLSummary != null) setTTLSummary.record(timeToLive);
                    if (cd != null) {
                        if (setDataSizeSummary == null) this.setDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-SetData-Size", _appName, null);
                        if (setDataSizeSummary != null) this.setDataSizeSummary.record(cd.getData().length);
                    }
                }
                final Future<Boolean> future = client.set(canonicalKey, cd, timeToLive, latch);
                if (log.isDebugEnabled() && shouldLog()) log.debug("SET : APP " + _appName + ", Future " + future + " for key : " + canonicalKey);
            }
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setTTL(timeToLive);
                event.setCachedData(cd);
                event.setLatch(latch);
                endEvent(event);
            }
            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception setting the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) endEvent(event);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName);
            throw new EVCacheException("Exception setting data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("SET : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + canonicalKey);
        }
    }

    public <T> EVCacheFuture[] append(String key, T value, int timeToLive) throws EVCacheException {
        return this.append(key, value, null, timeToLive);
    }

    public <T> EVCacheFuture[] append(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to set the data");
            return new EVCacheFuture[0]; // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.APPEND);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheFuture[0];
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.APPEND, stats, Operation.TYPE.MILLI);
        try {
            final EVCacheFuture[] futures = new EVCacheFuture[clients.length];
            CachedData cd = null;
            int index = 0;
            for (EVCacheClient client : clients) {
                if (cd == null) {
                    if (tc != null) {
                        cd = tc.encode(value);
                    } else if ( _transcoder != null) { 
                        cd = ((Transcoder<Object>)_transcoder).encode(value);
                    } else {
                        cd = client.getTranscoder().encode(value);
                    }
                    if (cd != null) {
                        if (appendDataSizeSummary == null) this.appendDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-AppendData-Size", _appName, null);
                        if (appendDataSizeSummary != null) this.appendDataSizeSummary.record(cd.getData().length);
                    }
                }
                final Future<Boolean> future = client.append(canonicalKey, cd);
                futures[index++] = new EVCacheFuture(future, key, _appName, client.getServerGroup());
            }
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setCachedData(cd);
                event.setTTL(timeToLive);
                endEvent(event);
            }
            touchData(canonicalKey, key, timeToLive, clients);
            return futures;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception setting the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheFuture[0];
            throw new EVCacheException("Exception setting data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("APPEND : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + canonicalKey);
        }
    }

    public <T> EVCacheFuture[] set(String key, T value, Transcoder<T> tc) throws EVCacheException {
        return this.set(key, value, tc, _timeToLive);
    }

    public <T> EVCacheFuture[] set(String key, T value, int timeToLive) throws EVCacheException {
        return this.set(key, value, (Transcoder<T>) _transcoder, timeToLive);
    }

    public <T> EVCacheFuture[] set(String key, T value) throws EVCacheException {
        return this.set(key, value, (Transcoder<T>) _transcoder, _timeToLive);
    }

    public EVCacheFuture[] delete(String key) throws EVCacheException {
    	final EVCacheLatch latch = this.delete(key, null);
        if (latch == null) return new EVCacheFuture[0];
        final List<Future<Boolean>> futures = latch.getAllFutures();
        if (futures == null || futures.isEmpty()) return new EVCacheFuture[0];
        final EVCacheFuture[] eFutures = new EVCacheFuture[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            final Future<Boolean> future = futures.get(i);
            if (future instanceof EVCacheFuture) {
                eFutures[i] = (EVCacheFuture) future;
            } else if (future instanceof EVCacheOperationFuture) {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, ((EVCacheOperationFuture<Boolean>) futures.get(i)).getServerGroup());
            } else {
                eFutures[i] = new EVCacheFuture(futures.get(i), key, _appName, null);
            }
        }
        return eFutures;

    }
    
	@Override
	public <T> EVCacheLatch delete(String key, Policy policy) throws EVCacheException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to delete the keyAPP " + _appName
                    + ", Key " + key);
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.DELETE);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.DELETE, stats);
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy == null ? Policy.ALL_MINUS_1 : policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);
        try {
            for (int i = 0; i < clients.length; i++) {
                Future<Boolean> future = clients[i].delete(canonicalKey, latch);
                if (log.isDebugEnabled() && shouldLog()) log.debug("DELETE : APP " + _appName + ", Future " + future + " for key : " + canonicalKey);
            }

            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setLatch(latch);
                endEvent(event);
            }
            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception while deleting the data for APP " + _appName + ", key : " + key, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName);
            throw new EVCacheException("Exception while deleting the data for APP " + _appName + ", key : " + key, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("DELETE : APP " + _appName + " Took " + op.getDuration() + " milliSec for key : " + key);
        }
	}

    

    public int getDefaultTTL() {
        return _timeToLive;
    }

    public long incr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        if ((null == key) || by < 0 || defaultVal < 0 || timeToLive < 0) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (log.isDebugEnabled() && shouldLog()) log.debug("INCR : " + _metricName + ":NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to incr the data");
            return -1;
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key),
                Call.INCR);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return -1;
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return -1;
            }
            startEvent(event);
        }

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.INCR, stats, Operation.TYPE.MILLI);
        try {
            final long[] vals = new long[clients.length];
            final String canonicalKey = getCanonicalizedKey(key);
            int index = 0;
            long currentValue = -1;
            for (EVCacheClient client : clients) {
                vals[index] = client.incr(canonicalKey, by, defaultVal, timeToLive);
                if (vals[index] != -1 && currentValue < vals[index]) currentValue = vals[index];
                index++;
            }

            if (currentValue != -1) {
                if (log.isDebugEnabled()) log.debug("INCR : APP " + _appName + " current value = " + currentValue + " for key : " + key);
                for (int i = 0; i < vals.length; i++) {
                    if (vals[i] == -1 && currentValue > -1) {
                        if (log.isDebugEnabled()) log.debug("INCR : APP " + _appName + "; Zone " + clients[i].getZone()
                                + " had a value = -1 so setting it to current value = " + currentValue + " for key : " + key);
                        clients[i].incr(canonicalKey, 0, currentValue, timeToLive);
                    } else if (vals[i] != currentValue) {
                        if (log.isDebugEnabled()) log.debug("INCR : APP " + _appName + "; Zone " + clients[i].getZone()
                                + " had a value of " + vals[i] + " so setting it to current value = " + currentValue + " for key : " + key);
                        clients[i].set(canonicalKey, String.valueOf(currentValue), timeToLive);
                    }
                }
            }
            if (event != null) endEvent(event);
            return currentValue;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception incrementing the value for APP " + _appName + ", key : " + key, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return -1;
            throw new EVCacheException("Exception incrementing value for APP " + _appName + ", key : " + key, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("INCR : APP " + _appName + ", Took " + op.getDuration()
            + " milliSec for key : " + key);
        }
    }

    public long decr(String key, long by, long defaultVal, int timeToLive) throws EVCacheException {
        if ((null == key) || by < 0 || defaultVal < 0 || timeToLive < 0) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (log.isDebugEnabled() && shouldLog()) log.debug("DECR : " + _metricName + ":NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to decr the data");
            return -1;
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key),
                Call.DECR);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return -1;
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return -1;
            }
            startEvent(event);
        }

        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.DECR, stats, Operation.TYPE.MILLI);
        try {
            final long[] vals = new long[clients.length];
            final String canonicalKey = getCanonicalizedKey(key);
            int index = 0;
            long currentValue = -1;
            for (EVCacheClient client : clients) {
                vals[index] = client.decr(canonicalKey, by, defaultVal, timeToLive);
                if (vals[index] != -1 && currentValue < vals[index]) currentValue = vals[index];
                index++;
            }

            if (currentValue != -1) {
                if (log.isDebugEnabled()) log.debug("DECR : APP " + _appName + " current value = " + currentValue
                        + " for key : " + key);
                for (int i = 0; i < vals.length; i++) {
                    if (vals[i] == -1 && currentValue > -1) {
                        if (log.isDebugEnabled()) log.debug("DECR : APP " + _appName + "; Zone " + clients[i].getZone()
                                + " had a value = -1 so setting it to current value = "
                                + currentValue + " for key : " + key);
                        clients[i].decr(canonicalKey, 0, currentValue, timeToLive);
                    } else if (vals[i] != currentValue) {
                        if (log.isDebugEnabled()) log.debug("DECR : APP " + _appName + "; Zone " + clients[i].getZone()
                                + " had a value of " + vals[i]
                                        + " so setting it to current value = " + currentValue + " for key : " + key);
                        clients[i].set(canonicalKey, String.valueOf(currentValue), timeToLive);
                    }
                }
            }

            if (event != null) endEvent(event);
            return currentValue;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception decrementing the value for APP " + _appName + ", key : " + key, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return -1;
            throw new EVCacheException("Exception decrementing value for APP " + _appName + ", key : " + key, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("DECR : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + key);
        }
    }

    @Override
    public <T> EVCacheLatch replace(String key, T value, Policy policy) throws EVCacheException {
        return replace(key, value, (Transcoder<T>) _transcoder, policy);
    }

    @Override
    public <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, Policy policy) throws EVCacheException {
        return replace(key, value, (Transcoder<T>) _transcoder, _timeToLive, policy);
    }

    public <T> EVCacheLatch replace(String key, T value,  int timeToLive, Policy policy) throws EVCacheException {
        return replace(key, value, (Transcoder<T>)_transcoder, timeToLive, policy);
    }

    @Override
    public <T> EVCacheLatch replace(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy)
            throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to set the data");
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key),
                Call.REPLACE);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName);
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.REPLACE, stats, Operation.TYPE.MILLI);
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy == null ? Policy.ALL_MINUS_1 : policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);
        try {
            final EVCacheFuture[] futures = new EVCacheFuture[clients.length];
            CachedData cd = null;
            int index = 0;
            for (EVCacheClient client : clients) {
                if (cd == null) {
                    if (tc != null) {
                        cd = tc.encode(value);
                    } else if ( _transcoder != null) { 
                        cd = ((Transcoder<Object>)_transcoder).encode(value);
                    } else {
                        cd = client.getTranscoder().encode(value);
                    }

                    if (replaceTTLSummary == null) this.replaceTTLSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-ReplaceData-TTL", _appName, null);
                    if (replaceTTLSummary != null) replaceTTLSummary.record(timeToLive);
                    if (cd != null) {
                        if (replaceDataSizeSummary == null) this.replaceDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-ReplaceData-Size", _appName, null);
                        if (replaceDataSizeSummary != null) this.replaceDataSizeSummary.record(cd.getData().length);
                    }
                }
                final Future<Boolean> future = client.replace(canonicalKey, cd, timeToLive, latch);
                futures[index++] = new EVCacheFuture(future, key, _appName, client.getServerGroup());
            }
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setTTL(timeToLive);
                event.setCachedData(cd);
                event.setLatch(latch);
                endEvent(event);
            }
            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception setting the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName);
            throw new EVCacheException("Exception setting data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("REPLACE : APP " + _appName + ", Took " + op .getDuration() + " milliSec for key : " + canonicalKey);
        }
    }

    @Override
    public String getCachePrefix() {
        return _cacheName;
    }

    public String getAppName() {
        return _appName;
    }

    public String getCacheName() {
        return _cacheName;
    }

    public <T> EVCacheLatch appendOrAdd(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy) throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to appendOrAdd the data");
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.APPEND_OR_ADD);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }
        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.APPEND_OR_ADD, stats, Operation.TYPE.MILLI);
        final EVCacheLatchImpl latch = new EVCacheLatchImpl(policy == null ? Policy.ALL_MINUS_1 : policy, clients.length - _pool.getWriteOnlyEVCacheClients().length, _appName);
        try {
            CachedData cd = null;
            for (EVCacheClient client : clients) {
                if (cd == null) {
                    if (tc != null) {
                        cd = tc.encode(value);
                    } else if ( _transcoder != null) { 
                        cd = ((Transcoder<Object>)_transcoder).encode(value);
                    } else {
                        cd = client.getTranscoder().encode(value);
                    }
                    if (cd != null) {
                        if (appendDataSizeSummary == null) this.appendDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-AppendData-Size", _appName, null);
                        if (appendDataSizeSummary != null) this.appendDataSizeSummary.record(cd.getData().length);
                    }
                }
                final Future<Boolean> future = client.appendOrAdd(canonicalKey, cd, timeToLive, latch);
                if (log.isDebugEnabled() && shouldLog()) log.debug("APPEND_OR_ADD : APP " + _appName + ", Future " + future + " for key : " + canonicalKey);
            }
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setTTL(timeToLive);
                event.setCachedData(cd);
                event.setLatch(latch);
                endEvent(event);
            }
            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception while appendOrAdd the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName);
            throw new EVCacheException("Exception while appendOrAdd data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("APPEND_OR_ADD : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + canonicalKey);
        }
    }

    @Override
    public <T> Future<Boolean>[] appendOrAdd(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to set the data");
            return new EVCacheFuture[0]; // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.APPEND_OR_ADD);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheFuture[0];
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return null;
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.APPEND_OR_ADD, stats, Operation.TYPE.MILLI);
        try {
            final EVCacheFuture[] futures = new EVCacheFuture[clients.length];
            CachedData cd = null;
            int index = 0;
            for (EVCacheClient client : clients) {
                if (cd == null) {
                    if (tc != null) {
                        cd = tc.encode(value);
                    } else if ( _transcoder != null) { 
                        cd = ((Transcoder<Object>)_transcoder).encode(value);
                    } else {
                        cd = client.getTranscoder().encode(value);
                    }
                    if (cd != null) {
                        if (appendDataSizeSummary == null) this.appendDataSizeSummary = EVCacheMetricsFactory.getDistributionSummary(_appName + "-AppendData-Size", _appName, null);
                        if (appendDataSizeSummary != null) this.appendDataSizeSummary.record(cd.getData().length);
                    }
                }
                final Future<Boolean> future = client.append(canonicalKey, cd);
                futures[index++] = new EVCacheFuture(future, key, _appName, client.getServerGroup(), client);
            }
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setCachedData(cd);
                event.setTTL(timeToLive);
                endEvent(event);
            }

            for(int i = 0; i < futures.length; i++) {
                final EVCacheFuture future = futures[i];

                if(!future.get()) {
                    final EVCacheClient client = future.getEVCacheClient();
                    if(client != null) {
                        final Future<Boolean> f = client.add(canonicalKey, timeToLive, cd);
                        futures[i] = new EVCacheFuture(f, key, _appName, client.getServerGroup(), client);
                    }
                }
            }

            touchData(canonicalKey, key, timeToLive, clients);
            return futures;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception setting the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheFuture[0];
            throw new EVCacheException("Exception setting data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("APPEND_OR_ADD : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + canonicalKey);
        }
    }
    
    public <T> boolean add(String key, T value, Transcoder<T> tc, int timeToLive) throws EVCacheException {
        final EVCacheLatch latch = add(key, value, tc, timeToLive, Policy.NONE);
        try {
            latch.await(_pool.getOperationTimeout().get(), TimeUnit.MILLISECONDS);
            return (latch.getSuccessCount() >= latch.getExpectedSuccessCount());
        } catch (InterruptedException e) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception adding the data for APP " + _appName + ", key : " + key, e);
            final boolean throwExc = doThrowException();
            if(throwExc) throw new EVCacheException("Exception add data for APP " + _appName + ", key : " + key, e);
            return false;
        }
    }

    @Override
    public <T> EVCacheLatch add(String key, T value, Transcoder<T> tc, int timeToLive, Policy policy) throws EVCacheException {
        if ((null == key) || (null == value)) throw new IllegalArgumentException();

        final boolean throwExc = doThrowException();
        final EVCacheClient[] clients = _pool.getEVCacheClientForWrite();
        if (clients.length == 0) {
            increment("NULL_CLIENT");
            if (throwExc) throw new EVCacheException("Could not find a client to Add the data");
            return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
        }

        final EVCacheEvent event = createEVCacheEvent(Arrays.asList(clients), Collections.singletonList(key), Call.ADD);
        if (event != null) {
            try {
                if (shouldThrottle(event)) {
                    increment("THROTTLED");
                    if (throwExc) throw new EVCacheException("Request Throttled for app " + _appName + " & key " + key);
                    return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
                }
            } catch(EVCacheException ex) {
                if(throwExc) throw ex;
                increment("THROTTLED");
                return new EVCacheLatchImpl(policy, 0, _appName); // Fast failure
            }
            startEvent(event);
        }

        final String canonicalKey = getCanonicalizedKey(key);
        final Operation op = EVCacheMetricsFactory.getOperation(_metricName, Call.ADD, stats, Operation.TYPE.MILLI);
        EVCacheLatch latch = null;
        try {
            CachedData cd = null;
            if (cd == null) {
                if (tc != null) {
                    cd = tc.encode(value);
                } else if ( _transcoder != null) { 
                    cd = ((Transcoder<Object>)_transcoder).encode(value);
                } else {
                    cd = _pool.getEVCacheClientForRead().getTranscoder().encode(value);
                }
            }
            if(clientUtil == null) clientUtil = new EVCacheClientUtil(_pool);
            latch = clientUtil.add(canonicalKey, cd, timeToLive, policy);
            if (event != null) {
                event.setCanonicalKeys(Arrays.asList(canonicalKey));
                event.setTTL(timeToLive);
                event.setCachedData(cd);
                event.setLatch(latch);
                endEvent(event);
            }

            return latch;
        } catch (Exception ex) {
            if (log.isDebugEnabled() && shouldLog()) log.debug("Exception adding the data for APP " + _appName + ", key : " + canonicalKey, ex);
            if (event != null) eventError(event, ex);
            if (!throwExc) return new EVCacheLatchImpl(policy, 0, _appName); 
            throw new EVCacheException("Exception adding data for APP " + _appName + ", key : " + canonicalKey, ex);
        } finally {
            op.stop();
            if (log.isDebugEnabled() && shouldLog()) log.debug("ADD : APP " + _appName + ", Took " + op.getDuration() + " milliSec for key : " + canonicalKey);
        }
    }

}