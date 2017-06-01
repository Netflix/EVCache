package com.netflix.evcache.service.resources;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.service.transcoder.RESTServiceTranscoder;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.util.concurrent.NFExecutorPool;

import net.spy.memcached.CachedData;


/**
 * Created by senugula on 3/22/16.
 */

@Singleton
@Path("/evcrest/v1.0")
public class EVCacheRESTService {

    private static final Logger logger = LoggerFactory.getLogger(EVCacheRESTService.class);
    private final EVCache.Builder builder;
    private final Map<String, EVCache> evCacheMap;
    private final RESTServiceTranscoder evcacheTranscoder = new RESTServiceTranscoder();
    private final ObjectMapper mapper = new ObjectMapper();
    private final NFExecutorPool pool;

    @Inject
    public EVCacheRESTService(EVCache.Builder builder) {
        this.builder = builder;
        this.evCacheMap = new ConcurrentHashMap<String, EVCache>();

        final int poolSize = DynamicPropertyFactory.getInstance().getIntProperty("EVCacheRESTService.async.pool.size", 10).get();

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(1000);
        this.pool = new NFExecutorPool("EVCacheRESTService-asyncBulkProcessor", poolSize, poolSize * 2, 30, TimeUnit.SECONDS, queue);
        pool.prestartAllCoreThreads();
        pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                r.run();

            }
        });

        final MonitorConfig config = MonitorConfig.builder("EVCacheRESTService.queue.size").withTag(DataSourceType.GAUGE).build();

        final LongGauge sizeCounter = new LongGauge(config) {
            @Override
            public Number getValue() {
                return Integer.valueOf(pool.getQueue().size());
            }

            @Override
            public Number getValue(int pollerIndex) {
                return getValue();
            }
        };
        DefaultMonitorRegistry.getInstance().register(sizeCounter);
    }

    @POST
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response setOperation(final InputStream in, @PathParam("appId") String pAppId, @PathParam("key") String key,
            @QueryParam("ttl") String ttl, @DefaultValue("") @QueryParam("flag") String flag,
            @DefaultValue("false") @QueryParam("async") String async) {
        try {
            final String appId = pAppId.toUpperCase();
            final byte[] bytes = IOUtils.toByteArray(in);
            return setData(appId, ttl, flag, key, bytes, Boolean.valueOf(async).booleanValue());
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("bulk/{appId}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response bulkPostOperation(final InputStream in, @PathParam("appId") String pAppId, @DefaultValue("false") @QueryParam("async") String async, @DefaultValue("") @HeaderParam("Content-Encoding") String encoding) {
        return processBulkSetOperation(in, pAppId, Boolean.valueOf(async).booleanValue(), encoding);
    }

    @PUT
    @Path("bulk/{appId}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response bulkPutOperation(final InputStream in, @PathParam("appId") String pAppId, @DefaultValue("false") @QueryParam("async") String async, @DefaultValue("") @HeaderParam("Content-Encoding") String encoding) {
        return processBulkSetOperation(in, pAppId, Boolean.valueOf(async).booleanValue(), encoding);
    }

    private Response processBulkSetOperation(InputStream in, final String pAppId, final boolean async, final String cEndoding) {
        try {
            final String appId = pAppId.toUpperCase();
            final String input;
            if(cEndoding.equals("gzip")) {
                InflaterInputStream inflaterInputStream = new InflaterInputStream(new ByteArrayInputStream(IOUtils.toByteArray(in)), new Inflater(false));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inflaterInputStream , "UTF-8"));
                String read;
                final StringBuilder sb = new StringBuilder();
                while ((read = bufferedReader.readLine()) != null) {
                    sb.append(read);
                }
                 input = sb.toString();
            } else {
                 input = IOUtils.toString(in, "UTF-8");
            }
            if(input.isEmpty() || input.length() == 0) {
                return Response.notModified("Input is empty").build();
            }
            if(async) {
                // Add to input queue and process
                final BulkProcessor op = new BulkProcessor(appId, input);
                pool.submit(op);
                return Response.status(202).build();
            } else {
                return bulkSetProcessor(input, appId, async);
            }
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
    }

    private Response bulkSetProcessor(String input, String appId, boolean async) throws Exception {
        long start = System.currentTimeMillis();
        JsonNode jsonObject = mapper.readTree(input);
        if(logger.isDebugEnabled()) logger.debug("Time to deserialize - " + (System.currentTimeMillis() - start));
        final String ttl = jsonObject.get("ttl").asText("");
        final String flag = jsonObject.has("flag") ? jsonObject.get("flag").asText("") : "0" ;
        final StringBuilder errorKeys = new StringBuilder();
        for(JsonNode obj : jsonObject.get("keys")) {
            final String key = obj.get("key").asText();
            final JsonNode val = obj.get("value");
            final byte[] data; 
            if(val.isTextual()) {
                data = val.asText().getBytes();
            } else {
                data = mapper.writeValueAsBytes(val);
            }
            final Response response = setData(appId, ttl, flag, key, data, async);
            if(!(response.getStatus() >= 200 && response.getStatus() < 300)) {
                errorKeys.append(key +";");
            }
        }
        if(logger.isDebugEnabled()) logger.debug("total Time taken for op - " + (System.currentTimeMillis() - start));
        if(errorKeys.length() > 0) return Response.notModified(errorKeys.toString()).build();
        return Response.ok("Bulk Set Operation was successful.").build();

    }

    @PUT
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response putOperation(final InputStream in, @PathParam("appId") String pAppId, @PathParam("key") String key,
            @QueryParam("ttl") String ttl, @DefaultValue("") @QueryParam("flag") String flag, 
            @DefaultValue("false") @QueryParam("async") String async) {
        try {
            final String appId = pAppId.toUpperCase();
            final byte[] bytes = IOUtils.toByteArray(in);
            return setData(appId, ttl, flag, key, bytes, Boolean.valueOf(async).booleanValue());
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
    }

    private Response setData(String appId, String ttl, String flag, String key, byte[] bytes, boolean async) throws EVCacheException, InterruptedException {
        final EVCache evcache = getEVCache(appId);
        if (ttl == null) {
            return Response.status(400).type(MediaType.TEXT_PLAIN).entity("Please specify ttl for the key " + key + " as query parameter \n").build();
        }
        final int timeToLive = Integer.valueOf(ttl).intValue();
        EVCacheLatch latch = null; 
        if(flag != null && flag.length() > 0) {
            final CachedData cd = new CachedData(Integer.parseInt(flag), bytes, Integer.MAX_VALUE);
            latch = evcache.set(key, evcacheTranscoder.encode(cd), timeToLive, Policy.ALL_MINUS_1);
        } else {
            latch = evcache.set(key, bytes, timeToLive, Policy.ALL_MINUS_1);
        }

        if(async) return Response.status(202).build();

        if(latch != null) {
            final boolean status = latch.await(2500, TimeUnit.MILLISECONDS);
            if(status) {
                return Response.ok("Set Operation for Key - " + key + " was successful. \n").build();
            } else {
                if(latch.getCompletedCount() > 0) {
                    if(latch.getSuccessCount() == 0){
                        return Response.serverError().build();
                    } else if(latch.getSuccessCount() > 0 ) {
                        return Response.ok("Set Operation for Key - " + key + " was successful in " + latch.getSuccessCount() + " Server Groups. \n").build();
                    }
                } else {
                    return Response.serverError().build();
                }
            }
        }
        return Response.serverError().build();
    }

    @GET
    @Path("incr/{appId}/{key}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response incrOperation(@PathParam("appId") String appId, @PathParam("key") String key, @DefaultValue("1") @QueryParam("by") String byStr, 
            @DefaultValue("1") @QueryParam("def") String defStr, @DefaultValue("0") @QueryParam("ttl") String ttlStr) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            final long by = Long.parseLong(byStr);
            final long def = Long.parseLong(defStr);
            final int ttl = Integer.parseInt(ttlStr);
            final long val = evCache.incr(key, by, def, ttl);
            return Response.status(200).type(MediaType.TEXT_PLAIN).entity(String.valueOf(val)).build();
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }


    @GET
    @Path("decr/{appId}/{key}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response decrOperation(@PathParam("appId") String appId, @PathParam("key") String key, @DefaultValue("1") @QueryParam("by") String byStr, 
            @DefaultValue("1") @QueryParam("def") String defStr, @DefaultValue("0") @QueryParam("ttl") String ttlStr) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            final long by = Long.parseLong(byStr);
            final long def = Long.parseLong(defStr);
            final int ttl = Integer.parseInt(ttlStr);
            final long val = evCache.decr(key, by, def, ttl);
            return Response.status(200).type(MediaType.TEXT_PLAIN).entity(String.valueOf(val)).build();
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }

    @GET
    @Path("{appId}/{key}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM})
    public Response getOperation(@PathParam("appId") String appId, @PathParam("key") String key) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            CachedData cachedData = (CachedData) evCache.get(key, evcacheTranscoder);
            if (cachedData == null) {
                return Response.status(404).type(MediaType.TEXT_PLAIN).entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            }
            byte[] bytes = cachedData.getData();
            int flag = cachedData.getFlags();
            if (bytes == null) {
                return Response.status(404).type(MediaType.TEXT_PLAIN).entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            } else {
                return Response.status(200).type(MediaType.APPLICATION_OCTET_STREAM).entity(bytes).header("X-EVCache-Flags", flag).build();
            }
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();

        }
    }


    @DELETE
    @Path("{appId}/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response deleteOperation(@PathParam("appId") String appId, @PathParam("key") String key) {
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        appId = appId.toUpperCase();
        final EVCache evCache = getEVCache(appId);
        try {
            Future<Boolean>[] _future = evCache.delete(key);
            if (_future.equals(Boolean.TRUE)) {
                if (logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Deleted Operation for Key - " + key + " was successful. \n").build();
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }

    private EVCache getEVCache(String appId) {
        EVCache evCache = evCacheMap.get(appId);
        if (evCache != null) return evCache;
        evCache = builder.setAppName(appId).build();
        evCacheMap.put(appId, evCache);
        return evCache;
    }

    class BulkProcessor  implements Runnable {

        private final String appId;
        private final String input;

        public BulkProcessor(String appId, String input) {
            this.appId = appId;
            this.input = input;
        }


        public String getAppId() {
            return appId;
        }


        public String getInput() {
            return input;
        }



        @Override
        public void run() {
            try {
                bulkSetProcessor(input, appId, true);
            } catch (Exception e) {
                logger.error("Exception processing the json", e);
            }
        }
    }

    /*
    public static void main(String args[]) {
        EVCacheRESTService rest = new EVCacheRESTService(null);
        try {
            rest.bulkSetProcessor("{\"ttl\": \"300\",\"keys\": [{\"key\": \"bulk-test-1\",\"ttl\": \"300\",\"value\": \"Fremont\"}, {\"key\": \"bulk-test-2\",\"ttl\": \"300\",\"value\": { \"city\": \"sanjose\", \"state\": \"ca\"}} ] }", "EVCACHE", false);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    */

}