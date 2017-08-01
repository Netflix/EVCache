package com.netflix.evcache.server.startup;

import com.netflix.karyon.spi.HealthCheckHandler;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by senugula on 10/12/16.
 */
//public class HealthCheckHandlerImpl implements com.netflix.karyon.spi.HealthCheckHandler {
    public class HealthCheckHandlerImpl implements HealthCheckHandler {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckHandlerImpl.class);
    private static transient MemcachedClient _client = null;
    private final static String _key = "__com.netflix.evcache.server.healthcheck";
    private final static String _value = "Greed is good!";
    private final static long _timeout = 5;

    static {
        try {
            _client = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses("localhost:11211"));
        } catch(Exception e) {
            logger.error("Unable to connect to local Memcached");
            e.printStackTrace();
        }
    }
    @Override
    public int getStatus() {
        try {
            if(checkHealth()) return 200;
        } catch (Exception e) {
            logger.error("Health failed");
            e.printStackTrace();
        }
        return 500;
    }

    protected boolean checkHealth() throws Exception {

        String rvalue = null;

        /* Phase 1: attempt to set (_key,_value) */
        try {
            Future<Boolean> status =  _client.set(_key, 0, _value);
            status.get();
        } catch (Exception e) {
            return(false);
        }

        /* Phase 2: attempt to get _key */
        try {
            Future<Object> r = _client.asyncGet(_key);
            try {
                rvalue = (String)r.get(_timeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                r.cancel(true);
                return(false);
            }
        } catch (Exception e) {
            return(false);
        }

        /* Phase 3: verify that the returned value is what we expect */
        if ((rvalue == null) || (!rvalue.equals(_value)))
            return(false);

        /* Success! */
        return(true);
    }
}

