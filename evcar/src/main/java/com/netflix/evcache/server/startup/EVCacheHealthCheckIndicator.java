package com.netflix.evcache.server.startup;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.config.DynamicIntProperty;
import com.netflix.runtime.health.api.Health;
import com.netflix.runtime.health.api.HealthIndicator;
import com.netflix.runtime.health.api.HealthIndicatorCallback;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 * Created by senugula on 10/26/16.
 */
@Singleton
public class EVCacheHealthCheckIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(EVCacheHealthCheckIndicator.class);
    private static final AtomicReference<MemcachedClient> _client = new AtomicReference<MemcachedClient>();
    private static final String _key = "__com.netflix.evcache.server.healthcheck";
    private static final String _value = "Greed is good!";
    private static final long _timeout = 5;

    private static final DynamicIntProperty failThreshold = new DynamicIntProperty("evcache.evcar.failthreshold", 6);

    private static final long FAIL_THRESHOLD = 6;
    private static AtomicLong consecutiveFailures = new AtomicLong(FAIL_THRESHOLD);
    private static AtomicLong lastHealthcheckSetTimeMs = new AtomicLong(0);
    private static final int healthcheckTtlSecs = 300;

    public EVCacheHealthCheckIndicator() {
    }

    @Override
    public void check(HealthIndicatorCallback healthIndicatorCallback) {
        int failThresholdNum = failThreshold.get();
        if (getStatus() == 200) {
            long prevVal = consecutiveFailures.getAndSet(0);
            if (prevVal > failThresholdNum) {
                logger.info("resetting consecutive failure count from " + prevVal);
            }
            healthIndicatorCallback.inform(Health.healthy().withDetail("message", "All Good!").build());
        } else {
            long failCount = consecutiveFailures.incrementAndGet();
            if (failCount > failThresholdNum) {
                healthIndicatorCallback.inform(Health.unhealthy().withDetail("message", "EVCacheHealth Failed").build());
            } else {
                healthIndicatorCallback.inform(Health.healthy().withDetail("message", "All Good!").build());
            }
        }
    }

    public int getStatus() {
        try {
            if (checkHealth()) return 200;
        } catch (Exception e) {
            logger.error("Health failed");
            e.printStackTrace();
        }
        return 500;
    }

    private synchronized MemcachedClient initClient() throws Exception {
        MemcachedClient client = _client.get();
        if (client == null) {
            client = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses("localhost:11211"));
            _client.set(client);
        }

        return client;
    }

    // if there is another thread doing a healthcheck when we do this, it might cause problems with the other thread, but it should eventually work
    private void shutdownClient() {
        MemcachedClient client = _client.getAndSet(null); // need to get new client later if unable to connect
        if (client != null) {
            client.shutdown();
        }
    }

    protected boolean checkHealth() throws Exception {
        MemcachedClient client = null;
        try {
            client = initClient();

            /* Phase 0: attempt to get key -- in case already present previously */
            try {
                // originally, we just did a get call without checking last set time, but that makes it less obvious which asg is writeonly mode when looking at hit rate metrics
                long healthcheckForceSetIntervalMs = (healthcheckTtlSecs-15)*1000;
                long now = System.currentTimeMillis();
                if ((now - lastHealthcheckSetTimeMs.get()) < healthcheckForceSetIntervalMs) {
                    boolean result = checkHealthcheckKey(client);
                    if (result) {
                        return true;
                    }
                }
                // somehow key value mismatched or wasn't there
                logger.info("get on healthcheck key failed on first attempt--possibly expired");

            } catch (Exception e) {
                // ignore and try again by doing set and get again below
                logger.info("get on healthcheck key failed on first attempt--exception occurred: " + e.getMessage());
                //shutdown client and reinit
                shutdownClient();
                client = initClient();
            }

            /* Phase 1: attempt to set (_key,_value) */
            Future<Boolean> status = client.set(_key, healthcheckTtlSecs, _value);
            status.get();
            lastHealthcheckSetTimeMs.set(System.currentTimeMillis());

            /* Phase 2: attempt to get _key */
            boolean result = checkHealthcheckKey(client);
            if (!result) {
                throw new Exception("healthcheck get on 2nd attempt failed");
            }

        } catch (Exception e) {
            logger.error("exception in healthcheck: ", e);
            shutdownClient();
            return false;
        }

        /* Success! */
        return true;
    }

    private boolean checkHealthcheckKey(MemcachedClient client) throws Exception {
        Future<Object> r = client.asyncGet(_key);
        try {
            String rvalue = (String) r.get(_timeout, TimeUnit.SECONDS);

            /* Phase 3: verify that the returned value is what we expect */
            if ((rvalue == null) || (!rvalue.equals(_value))) {
                return false;
            }
        } catch (Exception e) {
            r.cancel(true);
            throw e;
        }

        return true;
    }
}
